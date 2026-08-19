#!/bin/bash
# s3-verbosity-semantic-invariance.sh
source "$(dirname "$0")/../../lib/test-lib.sh"

create_tiny_warc_for_verbosity() {
    local path="$1"
    {
        printf 'WARC/1.0\r\n'
        printf 'WARC-Type: response\r\n'
        printf 'WARC-Record-ID: <urn:uuid:66666666-6666-6666-6666-666666666666>\r\n'
        printf 'WARC-Target-URI: http://example.com/verbosity\r\n'
        printf 'WARC-Date: 2026-01-01T00:00:00Z\r\n'
        printf 'Content-Type: application/http; msgtype=response\r\n'
        printf 'Content-Length: 76\r\n\r\n'
        printf 'HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n\r\nverbosity payload\r\n\r\n'
    } > "$path"
}

test_verbosity_modes_do_not_change_output_semantics() {
    mkdir -p "$TEST_OUTPUT_DIR"
    local input="$TEST_OUTPUT_DIR/verbosity-input.warc"
    local out_default="$TEST_OUTPUT_DIR/out-default.wet.gz"
    local out_silent="$TEST_OUTPUT_DIR/out-silent.wet.gz"
    local out_verbose="$TEST_OUTPUT_DIR/out-verbose.wet.gz"
    create_tiny_warc_for_verbosity "$input"

    "$WARC_CLI" extract-text "$input" "$out_default" >/dev/null 2>&1 || return 1
    "$WARC_CLI" extract-text "$input" "$out_silent" --silent >/dev/null 2>&1 || return 1
    "$WARC_CLI" extract-text "$input" "$out_verbose" --verbose >/dev/null 2>&1 || return 1

    local h_default h_silent h_verbose
    h_default=$(sha256sum "$out_default" | awk '{print $1}')
    h_silent=$(sha256sum "$out_silent" | awk '{print $1}')
    h_verbose=$(sha256sum "$out_verbose" | awk '{print $1}')

    [[ "$h_default" == "$h_silent" ]] || { log_fail "default vs silent output mismatch"; return 1; }
    [[ "$h_default" == "$h_verbose" ]] || { log_fail "default vs verbose output mismatch"; return 1; }
}

run_test test_verbosity_modes_do_not_change_output_semantics
