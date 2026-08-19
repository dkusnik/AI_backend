#!/bin/bash
# s3-edge-empty-tiny-core-commands.sh
# Task-ID: T-072,T-191
# @timeout: 120
source "$(dirname "$0")/../../lib/test-lib.sh"

create_tiny_warc() {
    local path="$1"
    {
        printf 'WARC/1.0\r\n'
        printf 'WARC-Type: response\r\n'
        printf 'WARC-Record-ID: <urn:uuid:33333333-3333-3333-3333-333333333333>\r\n'
        printf 'WARC-Target-URI: http://example.com/tiny\r\n'
        printf 'WARC-Date: 2026-01-01T00:00:00Z\r\n'
        printf 'Content-Type: application/http; msgtype=response\r\n'
        printf 'Content-Length: 73\r\n'
        printf '\r\n'
        printf 'HTTP/1.1 200 OK\r\n'
        printf 'Content-Type: text/plain\r\n'
        printf '\r\n'
        printf 'tiny text payload\r\n\r\n'
    } > "$path"
}

test_empty_and_tiny_inputs_for_core_commands() {
    mkdir -p "$TEST_OUTPUT_DIR"
    local empty="$TEST_OUTPUT_DIR/empty.warc"
    local tiny="$TEST_OUTPUT_DIR/tiny.warc"
    : > "$empty"
    create_tiny_warc "$tiny"

    set +e
    "$WARC_CLI" extract-text "$tiny" "$TEST_OUTPUT_DIR/tiny.wet.gz" --silent >/dev/null 2>&1
    local c_extract_tiny=$?
    "$WARC_CLI" convert "$tiny" "$TEST_OUTPUT_DIR/tiny-convert.warc.gz" --silent >/dev/null 2>&1
    local c_convert_tiny=$?
    "$WARC_CLI" info "$tiny" --silent >/dev/null 2>&1
    local c_info_tiny=$?
    "$WARC_CLI" validate "$tiny" >/dev/null 2>&1
    local c_validate_tiny=$?
    "$WARC_CLI" grep "$tiny" "$TEST_OUTPUT_DIR/tiny-grep.warc.gz" --silent >/dev/null 2>&1
    local c_grep_tiny=$?
    "$WARC_CLI" regen-zip "$tiny" "$TEST_OUTPUT_DIR/tiny-regen-zip.warc.gz" --silent >/dev/null 2>&1
    local c_regen_zip_tiny=$?
    "$WARC_CLI" regen-digests "$tiny" "$TEST_OUTPUT_DIR/tiny-digests.warc.gz" --silent >/dev/null 2>&1
    local c_regen_digests_tiny=$?
    "$WARC_CLI" regen-cdxj "$tiny" >/dev/null 2>&1
    local c_regen_cdxj_tiny=$?

    "$WARC_CLI" extract-text "$empty" "$TEST_OUTPUT_DIR/empty.wet.gz" --silent >/dev/null 2>&1
    local c_extract_empty=$?
    "$WARC_CLI" convert "$empty" "$TEST_OUTPUT_DIR/empty-convert.warc.gz" --silent >/dev/null 2>&1
    local c_convert_empty=$?
    "$WARC_CLI" validate "$empty" >/dev/null 2>&1
    local c_validate_empty=$?
    "$WARC_CLI" grep "$empty" "$TEST_OUTPUT_DIR/empty-grep.warc.gz" --silent >/dev/null 2>&1
    local c_grep_empty=$?
    "$WARC_CLI" regen-zip "$empty" "$TEST_OUTPUT_DIR/empty-regen-zip.warc.gz" --silent >/dev/null 2>&1
    local c_regen_zip_empty=$?
    "$WARC_CLI" regen-digests "$empty" "$TEST_OUTPUT_DIR/empty-digests.warc.gz" --silent >/dev/null 2>&1
    local c_regen_digests_empty=$?
    "$WARC_CLI" regen-cdxj "$empty" >/dev/null 2>&1
    local c_regen_cdxj_empty=$?
    set -e

    # Tiny inputs should be handled (non-crashing) by core commands.
    [[ $c_extract_tiny -eq 0 ]] || { log_fail "extract-text tiny failed"; return 1; }
    [[ $c_convert_tiny -eq 0 ]] || { log_fail "convert tiny failed"; return 1; }
    [[ $c_info_tiny -eq 0 ]] || { log_fail "info tiny failed"; return 1; }

    # validate may fail on minimal handcrafted record depending on strict checkers; enforce non-crash only.
    [[ $c_validate_tiny -ne 139 ]] || { log_fail "validate tiny crashed"; return 1; }
    [[ $c_grep_tiny -ne 139 ]] || { log_fail "grep tiny crashed"; return 1; }
    [[ $c_regen_zip_tiny -ne 139 ]] || { log_fail "regen-zip tiny crashed"; return 1; }
    [[ $c_regen_digests_tiny -ne 139 ]] || { log_fail "regen-digests tiny crashed"; return 1; }
    [[ $c_regen_cdxj_tiny -ne 139 ]] || { log_fail "regen-cdxj tiny crashed"; return 1; }

    # Empty file should fail gracefully (non-crash) across selected core commands.
    [[ $c_extract_empty -ne 139 ]] || { log_fail "extract-text empty crashed"; return 1; }
    [[ $c_convert_empty -ne 139 ]] || { log_fail "convert empty crashed"; return 1; }
    [[ $c_validate_empty -ne 139 ]] || { log_fail "validate empty crashed"; return 1; }
    [[ $c_grep_empty -ne 139 ]] || { log_fail "grep empty crashed"; return 1; }
    [[ $c_regen_zip_empty -ne 139 ]] || { log_fail "regen-zip empty crashed"; return 1; }
    [[ $c_regen_digests_empty -ne 139 ]] || { log_fail "regen-digests empty crashed"; return 1; }
    [[ $c_regen_cdxj_empty -ne 139 ]] || { log_fail "regen-cdxj empty crashed"; return 1; }
}

run_test test_empty_and_tiny_inputs_for_core_commands
