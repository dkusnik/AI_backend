#!/bin/bash
# Exercise reusable fault-injection and call-capture helpers.
# shellcheck disable=SC1091
source "$(dirname "$0")/../../lib/test-lib.sh"

test_fault_injection_helpers() {
    local fixtures="$TEST_OUTPUT_DIR/fault-fixtures"
    local unwritable="$fixtures/unwritable"
    local source_archive="$fixtures/source.warc.gz"
    local truncated_archive="$fixtures/truncated.warc.gz"
    local failing_stub="$fixtures/bin/fail"
    local sleeping_stub="$fixtures/bin/sleep"
    local fake_es_cli="$fixtures/app/bin/es-cli"
    local es_call_log="$fixtures/es-cli.calls"

    mkdir -p "$fixtures"

    make_unwritable_directory "$unwritable" || return 1
    [[ "$(stat -c '%a' "$unwritable")" == "555" ]] || {
        log_fail "Unwritable fixture retained write permission"
        return 1
    }

    printf '%s' 'WARC/1.0 fixture bytes that will be truncated' > "$source_archive"
    make_truncated_archive_fixture "$source_archive" "$truncated_archive" 11 || return 1
    [[ "$(wc -c < "$truncated_archive")" -eq 11 ]] || {
        log_fail "Truncated archive fixture has the wrong size"
        return 1
    }
    cmp -n 11 "$source_archive" "$truncated_archive" || {
        log_fail "Truncated archive fixture is not a source prefix"
        return 1
    }

    make_stub_executable "$failing_stub" exit-nonzero 23 || return 1
    "$failing_stub"
    local rc=$?
    assert_command_failure "$rc" "Non-zero stub unexpectedly succeeded" || return 1
    [[ "$rc" -eq 23 ]] || {
        log_fail "Non-zero stub returned $rc instead of 23"
        return 1
    }

    make_stub_executable "$sleeping_stub" sleep-forever || return 1
    timeout 0.1s "$sleeping_stub"
    rc=$?
    [[ "$rc" -eq 124 ]] || {
        log_fail "Sleeping stub did not remain blocked until timeout (rc=$rc)"
        return 1
    }

    make_fake_es_cli "$fake_es_cli" "$es_call_log" || return 1
    "$fake_es_cli" batch-delete nac-data-test 'query with spaces' || return 1
    [[ "$(cat "$es_call_log")" == 'batch-delete nac-data-test query\ with\ spaces' ]] || {
        log_fail "Fake es-cli did not preserve the call arguments"
        return 1
    }
}

test_call_capture_wrapper() {
    local fixtures="$TEST_OUTPUT_DIR/call-capture"
    local target="$fixtures/target"
    local wrapper="$fixtures/wrapper"
    local captures="$fixtures/captures"
    local invocation="$captures/invocation-000001"

    mkdir -p "$fixtures"
    # The single-quoted strings are source for the generated executable.
    # shellcheck disable=SC2016
    {
        printf '%s\n' '#!/bin/bash'
        printf '%s\n' 'printf "stdout:%s|%s" "$1" "$2"'
        printf '%s\n' 'printf "stderr:%s|%s" "$1" "$2" >&2'
        printf '%s\n' 'exit 17'
    } > "$target"
    chmod +x "$target"
    make_call_capture_wrapper "$wrapper" "$target" "$captures" || return 1

    if ! env -u ES_USER ES_URL='http://example.invalid:9200' ES_PASS= \
        "$wrapper" 'first argument' '' >/dev/null 2>/dev/null; then
        :
    else
        log_fail "Capture wrapper did not preserve the target failure"
        return 1
    fi

    assert_directory_exists "$invocation" || return 1
    mapfile -d '' -t captured_argv < "$invocation/argv.nul"
    [[ "${#captured_argv[@]}" -eq 2 ]] || {
        log_fail "Capture wrapper recorded ${#captured_argv[@]} arguments instead of 2"
        return 1
    }
    [[ "${captured_argv[0]}" == 'first argument' && "${captured_argv[1]}" == '' ]] || {
        log_fail "Capture wrapper changed the argument bytes"
        return 1
    }

    [[ "$(cat "$invocation/env/ES_URL.state")" == set ]] || return 1
    [[ "$(cat "$invocation/env/ES_URL.value")" == 'http://example.invalid:9200' ]] || return 1
    [[ "$(cat "$invocation/env/ES_USER.state")" == unset ]] || return 1
    [[ ! -e "$invocation/env/ES_USER.value" ]] || return 1
    [[ "$(cat "$invocation/env/ES_PASS.state")" == set ]] || return 1
    [[ -f "$invocation/env/ES_PASS.value" && ! -s "$invocation/env/ES_PASS.value" ]] || return 1
    [[ "$(cat "$invocation/stdout")" == 'stdout:first argument|' ]] || return 1
    [[ "$(cat "$invocation/stderr")" == 'stderr:first argument|' ]] || return 1
    [[ "$(cat "$invocation/exit-status")" == 17 ]] || return 1
}

run_test test_fault_injection_helpers
run_test test_call_capture_wrapper
