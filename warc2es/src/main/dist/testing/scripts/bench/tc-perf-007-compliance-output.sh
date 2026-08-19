#!/bin/bash
# tc-perf-007-compliance-output.sh
source "$(dirname "$0")/../../lib/test-lib.sh"

compliance_output_invariants() {
    local input="$TEST_OUTPUT_DIR/compliance-output-input.warc"
    local output="$TEST_OUTPUT_DIR/compliance-output.wet.gz"

    mkdir -p "$TEST_OUTPUT_DIR"
    {
        printf 'WARC/1.0\r\n'
        printf 'WARC-Type: response\r\n'
        printf 'WARC-Record-ID: <urn:uuid:77777777-7777-7777-7777-777777777777>\r\n'
        printf 'WARC-Target-URI: http://example.com/compliance\r\n'
        printf 'WARC-Date: 2026-01-01T00:00:00Z\r\n'
        printf 'Content-Type: application/http; msgtype=response\r\n'
        printf 'Content-Length: 101\r\n'
        printf '\r\n'
        printf 'HTTP/1.1 200 OK\r\n'
        printf 'Content-Type: text/html\r\n'
        printf '\r\n'
        printf '<html><body><main><p>Compliance output payload.</p></main></body></html>\r\n\r\n'
    } > "$input"

    "$WARC_CLI" --profile=light-optimized extract-text "$input" "$output" --silent >/dev/null 2>&1 || {
        echo "TESTCASE|compliance-output|FAIL|extract-text failed"
        return 1
    }

    assert_file_exists "$output" || { echo "TESTCASE|compliance-output|FAIL|missing output"; return 1; }

    local text_len checksum
    text_len=$(gzip -dc "$output" | wc -c | tr -d ' ')
    checksum=$(sha256sum "$output" | awk '{print $1}')

    if [[ "$text_len" -le 0 ]]; then
        echo "TESTCASE|compliance-output|FAIL|empty output payload"
        return 1
    fi

    if [[ ${#checksum} -ne 64 ]]; then
        echo "TESTCASE|compliance-output|FAIL|invalid checksum"
        return 1
    fi

    echo "TESTCASE|compliance-output|PASS|bytes=${text_len},sha256=${checksum}"
    return 0
}

run_test compliance_output_invariants
