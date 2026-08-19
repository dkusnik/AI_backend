#!/bin/bash
# s3-repeat-run-idempotence.sh
source "$(dirname "$0")/../../lib/test-lib.sh"

create_tiny_warc_for_idempotence() {
    local path="$1"
    {
        printf 'WARC/1.0\r\n'
        printf 'WARC-Type: response\r\n'
        printf 'WARC-Record-ID: <urn:uuid:55555555-5555-5555-5555-555555555555>\r\n'
        printf 'WARC-Target-URI: http://example.com/idempotence\r\n'
        printf 'WARC-Date: 2026-01-01T00:00:00Z\r\n'
        printf 'Content-Type: application/http; msgtype=response\r\n'
        printf 'Content-Length: 74\r\n\r\n'
        printf 'HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n\r\nstable payload\r\n\r\n'
    } > "$path"
}

test_repeated_run_idempotence() {
    mkdir -p "$TEST_OUTPUT_DIR"
    local input="$TEST_OUTPUT_DIR/idempotence-input.warc"
    local out1="$TEST_OUTPUT_DIR/out-1.wet.gz"
    local out2="$TEST_OUTPUT_DIR/out-2.wet.gz"
    create_tiny_warc_for_idempotence "$input"

    "$WARC_CLI" extract-text "$input" "$out1" --silent >/dev/null 2>&1 || return 1
    "$WARC_CLI" extract-text "$input" "$out2" --silent >/dev/null 2>&1 || return 1

    local h1 h2
    h1=$(sha256sum "$out1" | awk '{print $1}')
    h2=$(sha256sum "$out2" | awk '{print $1}')

    [[ "$h1" == "$h2" ]] || { log_fail "output checksum differs across identical runs"; return 1; }
}

run_test test_repeated_run_idempotence
