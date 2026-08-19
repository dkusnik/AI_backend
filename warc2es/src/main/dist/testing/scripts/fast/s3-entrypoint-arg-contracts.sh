#!/bin/bash
# Keep operator argument failures explicit and help side-effect free.
source "$(dirname "$0")/../../lib/test-lib.sh"

test_entrypoint_arg_contracts() {
    local out_root="$PROJECT_ROOT/out"
    local tmp="$TEST_OUTPUT_DIR/entrypoint-args"
    local item entry flag stderr output rc
    local -a missing_value_cases=(
        'warc2wet|--url-id'
        'warc2wet|--crawl-id'
        'warc2wet|--result-format'
        'es-upsert|--stream'
        'es-upsert|--url-id'
        'es-upsert|--crawl-id'
        'es-upsert|--start-date'
        'es-upsert|--es-url'
        'es-delete|--stream'
        'es-delete|--url-id'
        'es-delete|--crawl-id'
        'es-delete|--es-url'
        'es-delete|--result-format'
        'es-reinit|--stream'
        'es-reinit|--es-url'
        'es-reinit|--result-format'
    )

    mkdir -p "$tmp/empty-input"

    for item in "${missing_value_cases[@]}"; do
        entry="${item%%|*}"
        flag="${item#*|}"
        set +e
        stderr=$("$out_root/$entry.sh" "$flag" 2>&1 >/dev/null)
        rc=$?
        set -e
        assert_command_failure "$rc" "$entry $flag without a value should fail" || return 1
        [[ "$stderr" == "Error: $flag requires a value" ]] || {
            log_fail "$entry $flag emitted the wrong stderr: $stderr"
            return 1
        }
    done

    set +e
    stderr=$("$out_root/es-upsert.sh" first.wet second.wet 2>&1 >/dev/null)
    rc=$?
    set -e
    assert_command_failure "$rc" "es-upsert should reject a second positional" || return 1
    [[ "$stderr" == "Error: unexpected positional argument: second.wet" ]] || {
        log_fail "es-upsert emitted the wrong second-positional error: $stderr"
        return 1
    }

    set +e
    output=$("$out_root/es-upsert.sh" "$tmp/empty-input" --url-id=u --crawl-id=c --dry-run 2>&1)
    rc=$?
    set -e
    assert_command_failure "$rc" "es-upsert should reject an empty directory" || return 1
    echo "$output" | grep -Fq "no .wet.gz files found" || {
        log_fail "es-upsert did not report the empty directory"
        return 1
    }

    for entry in warc2wet wet-merge es-upsert es-upsert-all es-delete es-reinit; do
        set +e
        "$out_root/$entry.sh" --help > "$tmp/$entry.stdout" 2> "$tmp/$entry.stderr"
        rc=$?
        set -e
        assert_command_success "$rc" "$entry --help should succeed" || return 1
        grep -Fq "Usage: $entry.sh" "$tmp/$entry.stdout" || {
            log_fail "$entry --help did not write usage to stdout"
            return 1
        }
        if [[ -s "$tmp/$entry.stderr" ]]; then
            log_fail "$entry --help wrote diagnostics to stderr"
            return 1
        fi
    done
}

run_test test_entrypoint_arg_contracts
