#!/bin/bash
# tc-conv-002-split-file.sh
source "$(dirname "$0")/../../../../lib/test-lib.sh"

test_split_file() {
    # Build synthetic input guaranteed to be well above the 10KB split threshold.
    # Use plain WARC + output.compression=none to avoid gzip variability.
    local input="$TEST_OUTPUT_DIR/input-split-large.warc"
    local output_base="$TEST_OUTPUT_DIR/conv-002-segment.warc"

    rm -f "$input"
    for i in $(seq 1 300); do
        local payload
        payload=$(printf '%0512d' "$i")
        {
            printf "WARC/1.0\r\n"
            printf "WARC-Type: resource\r\n"
            printf "WARC-Record-ID: <urn:uuid:00000000-0000-0000-0000-%012d>\r\n" "$i"
            printf "WARC-Date: 2026-01-01T00:00:00Z\r\n"
            printf "Content-Type: application/octet-stream\r\n"
            printf "Content-Length: 512\r\n"
            printf "\r\n"
            printf "%s\r\n\r\n" "$payload"
        } >> "$input"
    done

    # Check size
    local size=$(stat -c%s "$input")
    log_info "Input size: $size bytes"
    if [ "$size" -le 10240 ]; then
        log_fail "Synthetic input too small ($size bytes), expected > 10KB"
        return 1
    fi

    # 10KB threshold, intentionally below input size and above small-buffer noise.
    local limit=10240

    log_info "Running convert with --output-size-limit=$limit..."
    # Note: output file must not exist, or it will be treated as input
    rm -f "$output_base" "$TEST_OUTPUT_DIR/conv-002-segment"*.warc "$TEST_OUTPUT_DIR/conv-002-segment"*.warc.gz 2>/dev/null

    "$WARC_CLI" convert "$input" "$output_base" \
        --output-size-limit="$limit" \
        --output.compression=none

    assert_command_success $?

    # Output naming depends on splitting behavior, gather all segment variants.
    local files=($(find "$TEST_OUTPUT_DIR" -maxdepth 1 -type f \( -name 'conv-002-segment*.warc' -o -name 'conv-002-segment*.warc.gz' \) | sort))
    local count=${#files[@]}

    log_info "Created $count segment files"
    if [ "$count" -ge 2 ]; then
        log_success "Splitting happened (found $count files)"
        for f in "${files[@]}"; do
             local f_size=$(stat -c%s "$f")
             # Size might slightly exceed limit if record boundary requires it?
             # WARC writers usually try to stay under or close.
             log_info "  $f : $f_size bytes"
        done
    else
        log_fail "Did not split into multiple files (found $count). Limit was $limit"
        # This might fail if the implementation doesn't support splitting on mere concatenation or size is too small
        return 1
    fi
}

run_test test_split_file
