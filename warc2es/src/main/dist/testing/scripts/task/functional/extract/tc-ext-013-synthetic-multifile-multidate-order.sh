#!/bin/bash
# @timeout: 180
# T-214: Synthetic multifile/multidate extract
# - Build 5 synthetic input files with records from 2 dates
# - Records are intentionally out-of-order by digest in inputs
# - extract-text must produce:
#   * exactly 2 outputs: <prefix>-20260130.doet.gz and <prefix>-20260131.doet.gz
#   * sorted (DOET-style) records by digest within each output
#   * dedup-aware cardinality (0 < output count <= input count)
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

is_digest_sorted() {
    local file="$1"
    zcat "$file" | awk '
      BEGIN { prev=""; ok=1 }
      tolower($1) == "warc-payload-digest:" {
        d=$2
        gsub(/\r/, "", d)
        if (prev != "" && d < prev) { ok=0; exit }
        prev=d
      }
      END { exit(ok ? 0 : 1) }'
}

test_synthetic_multifile_multidate_order() {
    local in_dir="$TEST_OUTPUT_DIR/ext-013-input"
    local out_dir="$TEST_OUTPUT_DIR/ext-013-out"
    local prefix="syn013"
    mkdir -p "$in_dir" "$out_dir"

    # 5 input files, mixed dates and deliberately out-of-order digests.
    {
        create_response_record "sha256:0000000000000005" "http://x/a5" "2026-01-30T10:00:00Z" "A5 synthetic content line 0005"
        create_response_record "sha256:0000000000000002" "http://x/a2" "2026-01-30T10:01:00Z" "A2 synthetic content line 0002"
        create_response_record "sha256:0000000000000009" "http://x/b9" "2026-01-31T11:00:00Z" "B9 synthetic content line 0009"
    } | gzip > "$in_dir/syn-013-01.warc.gz"

    {
        create_response_record "sha256:0000000000000001" "http://x/a1" "2026-01-30T10:02:00Z" "A1 synthetic content line 0001"
        create_response_record "sha256:0000000000000008" "http://x/b8" "2026-01-31T11:01:00Z" "B8 synthetic content line 0008"
    } | gzip > "$in_dir/syn-013-02.warc.gz"

    {
        create_response_record "sha256:0000000000000004" "http://x/a4" "2026-01-30T10:03:00Z" "A4 synthetic content line 0004"
        create_response_record "sha256:0000000000000006" "http://x/b6" "2026-01-31T11:02:00Z" "B6 synthetic content line 0006"
    } | gzip > "$in_dir/syn-013-03.warc.gz"

    {
        create_response_record "sha256:0000000000000003" "http://x/a3" "2026-01-30T10:04:00Z" "A3 synthetic content line 0003"
        create_response_record "sha256:0000000000000007" "http://x/b7" "2026-01-31T11:03:00Z" "B7 synthetic content line 0007"
    } | gzip > "$in_dir/syn-013-04.warc.gz"

    {
        create_response_record "sha256:0000000000000010" "http://x/b10" "2026-01-31T11:04:00Z" "B10 synthetic content line 0010"
        create_response_record "sha256:0000000000000011" "http://x/b11" "2026-01-31T11:05:00Z" "B11 synthetic content line 0011"
    } | gzip > "$in_dir/syn-013-05.warc.gz"

    local -a inputs=()
    while IFS= read -r f; do inputs+=("$f"); done < <(find "$in_dir" -maxdepth 1 -type f -name "*.warc.gz" | LC_ALL=C sort)
    if [[ ${#inputs[@]} -ne 5 ]]; then
        log_fail "Expected 5 synthetic input files, got ${#inputs[@]}"
        return 1
    fi

    local input_total=0
    local f
    for f in "${inputs[@]}"; do
        input_total=$(( input_total + $(warc_type_count "$f" "response") ))
    done
    log_info "Synthetic inputs: files=${#inputs[@]} records=$input_total"

    "$WARC_CLI" extract-text "${inputs[@]}" --output-dir="$out_dir" --output-prefix="$prefix" --silent
    assert_command_success $? "extract-text synthetic multifile/multidate"

    local out_30="$out_dir/${prefix}-20260130.doet.gz"
    local out_31="$out_dir/${prefix}-20260131.doet.gz"
    assert_file_exists "$out_30"
    assert_file_exists "$out_31"

    # exactly 2 date buckets expected
    local out_count
    out_count=$(find "$out_dir" -maxdepth 1 -type f -name "${prefix}-*.doet.gz" | wc -l | tr -d '[:space:]')
    if [[ "$out_count" -ne 2 ]]; then
        log_fail "Expected exactly 2 output files, got $out_count"
        return 1
    fi

    local out_30_count out_31_count output_total
    out_30_count=$(warc_type_count "$out_30" "conversion")
    out_31_count=$(warc_type_count "$out_31" "conversion")
    output_total=$(( out_30_count + out_31_count ))

    if [[ "$output_total" -le 0 || "$output_total" -gt "$input_total" ]]; then
        log_fail "Unexpected output cardinality: input=$input_total output=$output_total"
        return 1
    fi

    is_digest_sorted "$out_30" || { log_fail "Digest order not sorted in $(basename "$out_30")"; return 1; }
    is_digest_sorted "$out_31" || { log_fail "Digest order not sorted in $(basename "$out_31")"; return 1; }

    log_info "Outputs sorted and complete: 20260130=$out_30_count 20260131=$out_31_count"
    return 0
}

run_test test_synthetic_multifile_multidate_order
