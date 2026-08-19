#!/bin/bash
# Guard root-level es-upsert layout detection and inherited child CWD.
source "$(dirname "$0")/../../lib/test-lib.sh"

test_es_upsert_runtime_layout_guard() {
    local script="$PROJECT_ROOT/src/main/dist/es-upsert.sh"
    assert_file_exists "$script" || return 1

    grep -Fq 'source "$RUNTIME_LIB"' "$script" || {
        log_fail "Expected es-upsert to source runtime-lib"
        return 1
    }
    grep -Fq 'runtime_resolve_layout "$SCRIPT_DIR"' "$script" || {
        log_fail "Expected shared runtime layout detection"
        return 1
    }
    grep -Fq 'ES_CLI="$APP_DIR/bin/es-cli"' "$script" || {
        log_fail "Expected ES_CLI to resolve from APP_DIR/bin"
        return 1
    }
    grep -Fq 'pair_dir="$RUNTIME_DIR/all/wet/$URL_ID/$CRAWL_ID"' "$script" || {
        log_fail "Expected published pair path to resolve below RUNTIME_DIR/all/wet"
        return 1
    }
    grep -Fq 'Error: --all is retired; use es-upsert-all.sh' "$script" || {
        log_fail "Expected retired --all mode to fail explicitly"
        return 1
    }
    if ! grep -Fq 'INPUT_PATH="$RUNTIME_DIR/wet/$URL_ID/$CRAWL_ID"' "$script"; then
        log_fail "Expected pair-scoped managed staging fallback"
        return 1
    fi

    local layout runtime installed library_dir bin_dir expected_cwd
    local input foreign call_log output rc actual_cwd
    for layout in source assembled; do
        runtime="$TEST_OUTPUT_DIR/$layout"
        installed="$runtime/es-upsert.sh"
        input="$runtime/input.wet.gz"
        foreign="$runtime/foreign-cwd"
        call_log="$runtime/es-cli.cwd"

        if [[ "$layout" == source ]]; then
            library_dir="$runtime/lib/scripts"
            bin_dir="$runtime/bin"
        else
            library_dir="$runtime/app/lib/scripts"
            bin_dir="$runtime/app/bin"
        fi
        expected_cwd="$library_dir"

        mkdir -p "$library_dir" "$bin_dir" "$foreign"
        cp "$script" "$installed"
        cp "$PROJECT_ROOT/src/main/dist/lib/scripts/runtime-lib.sh" "$library_dir/runtime-lib.sh"
cat > "$bin_dir/es-cli" <<'FAKE_ES'
#!/bin/bash
printf '%s\n' "$PWD" >> "$CALL_LOG"
if [[ "${1:-}" == batch-delete ]]; then
    printf '%s\n' '{"total":0,"deleted":0,"version_conflicts":0,"timed_out":false,"failures":[]}'
fi
exit 0
FAKE_ES
        chmod +x "$installed" "$bin_dir/es-cli"
        : > "$input"

        set +e
        output=$(cd "$foreign" && CALL_LOG="$call_log" "$installed" "$input" \
            --url-id=u --crawl-id=c 2>&1)
        rc=$?
        set -e
        assert_command_success "$rc" "es-upsert should run from a foreign CWD in $layout layout: $output" || return 1
        assert_file_exists "$call_log" || return 1
        actual_cwd="$(sort -u "$call_log")"
        if [[ "$actual_cwd" != "$expected_cwd" ]]; then
            log_fail "$layout layout child CWD changed: expected $expected_cwd, got $actual_cwd"
            return 1
        fi
    done
}

run_test test_es_upsert_runtime_layout_guard
