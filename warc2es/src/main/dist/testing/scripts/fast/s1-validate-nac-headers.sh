#!/bin/bash
# s1-validate-nac-headers.sh
source "$(dirname "$0")/../../lib/test-lib.sh"

write_derived_wet() {
    local target="$1"
    local include_provenance="$2"

    {
        printf 'WARC/1.0\r\n'
        printf 'WARC-Type: conversion\r\n'
        printf 'WARC-Record-ID: <urn:uuid:88888888-8888-4888-8888-888888888888>\r\n'
        printf 'WARC-Date: 2026-08-02T00:00:00Z\r\n'
        printf 'WARC-Target-URI: https://example.test/derived\r\n'
        if [[ "$include_provenance" == true ]]; then
            # Header names are deliberately mixed-case to verify case-insensitive matching.
            printf 'x-nAc-url-id: example_org\r\n'
            printf 'x-NaC-crawl-id: 2026_08\r\n'
        fi
        printf 'Content-Type: text/plain\r\n'
        printf 'Content-Length: 7\r\n\r\n'
        printf 'fixture\r\n\r\n'
    } > "$target"
}

test_validate_nac_headers() {
    local derived_wet="$TEST_OUTPUT_DIR/derived-with-provenance.wet"
    local missing_provenance_wet="$TEST_OUTPUT_DIR/derived-without-provenance.wet"
    local output
    local rc
    local failure_log

    write_derived_wet "$derived_wet" true || return 1
    write_derived_wet "$missing_provenance_wet" false || return 1

    set +e
    output=$("$WARC_CLI" validate "$derived_wet" 2>&1)
    rc=$?
    set -e

    assert_command_success "$rc" \
        "derived WET with case-insensitive X-NAC provenance should pass" || return 1
    echo "$output" | grep -Fq "$derived_wet: OK" || {
        log_fail "Expected derived WET to be reported as OK"
        return 1
    }

    set +e
    output=$("$WARC_CLI" validate "$missing_provenance_wet" 2>&1)
    rc=$?
    set -e

    assert_command_failure "$rc" \
        "derived WET without X-NAC provenance should fail" || return 1
    echo "$output" | grep -Fq "$missing_provenance_wet: FAIL" || {
        log_fail "Expected missing-provenance WET to be reported as FAIL"
        return 1
    }

    failure_log=$(echo "$output" | sed -n 's/^  log: //p')
    [[ -n "$failure_log" && -f "$failure_log" ]] || {
        log_fail "Expected validate to retain the failure log"
        return 1
    }
    grep -Fq 'missing X-NAC-* headers' "$failure_log" || {
        log_fail "Expected failure to come from the missing provenance-header check"
        return 1
    }
}

run_test test_validate_nac_headers
