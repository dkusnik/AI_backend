#!/bin/bash
# Guard explicit delete scope, exact published-WET cleanup, and low-level failures.
source "$(dirname "$0")/../../lib/test-lib.sh"

test_es_delete_match_all_requires_explicit_gate() {
    local script="$PROJECT_ROOT/out/es-delete.sh"
    local output rc
    assert_file_exists "$script" || return 1

    set +e
    output=$("$script" --stream=test --dry-run 2>&1)
    rc=$?
    set -e
    assert_command_failure "$rc" "delete without pair or --all-documents should fail" || return 1
    echo "$output" | grep -q -- "--all-documents\|--url-id" || {
        log_fail "Expected an explicit delete-scope diagnostic"
        return 1
    }

    set +e
    output=$("$script" --stream=test --all-documents --dry-run 2>&1)
    rc=$?
    set -e
    assert_command_success "$rc" "explicit match_all dry-run should succeed" || return 1
    echo "$output" | grep -q "match_all (--all-documents)" || {
        log_fail "Expected explicit match_all scope in dry-run output"
        return 1
    }
}

test_es_delete_pair_cleanup_uses_published_layout() {
    local runtime="$TEST_OUTPUT_DIR/runtime"
    local script="$runtime/es-delete.sh"
    local calls="$runtime/es-calls.log"
    local selected retained sha

    mkdir -p "$runtime/app/bin" "$runtime/app/lib/scripts" \
        "$runtime/all/wet/alpha/crawl1" "$runtime/all/wet/alpha/crawl2"
    cp "$PROJECT_ROOT/src/main/dist/es-delete.sh" "$script"
    cp "$PROJECT_ROOT/src/main/dist/lib/scripts/runtime-lib.sh" \
        "$runtime/app/lib/scripts/runtime-lib.sh"
    chmod +x "$script"
    cat > "$runtime/app/bin/es-cli" <<'FAKE_ES'
#!/bin/bash
echo "$*" >> "$(dirname "$0")/../../es-calls.log"
if [[ "${1:-}" == batch-delete ]]; then
    jq -cn '{total:0,deleted:0,version_conflicts:0,timed_out:false,failures:[]}'
fi
FAKE_ES
    chmod +x "$runtime/app/bin/es-cli"

    sha=$(printf selected | sha256sum | awk '{print $1}')
    selected="$runtime/all/wet/alpha/crawl1/$sha.wet.gz"
    printf selected > "$selected"
    sha=$(printf retained | sha256sum | awk '{print $1}')
    retained="$runtime/all/wet/alpha/crawl2/$sha.wet.gz"
    printf retained > "$retained"

    CALL_LOG="$calls" "$script" --stream=test --url-id=alpha --crawl-id=crawl1 >/dev/null

    [[ ! -e "$selected" && -e "$retained" ]] || {
        log_fail "Pair delete did not clean only all/wet/alpha/crawl1"
        return 1
    }
    [[ "$(grep -c '^batch-delete ' "$calls")" -eq 1 ]] || {
        log_fail "Expected exactly one es-cli batch-delete call"
        return 1
    }
}

test_es_cli_batch_delete_runtime_guard() {
    local es_cli="$PROJECT_ROOT/target/dist/bin/es-cli"
    local output rc
    assert_file_exists "$es_cli" || return 1

    set +e
    output=$(ES_URL=http://localhost:59999 "$es_cli" batch-delete my-index \
        '{"query":{"match_all":{}}}' 2>&1)
    rc=$?
    set -e
    assert_command_failure "$rc" "batch-delete against unreachable ES should fail" || return 1
    if echo "$output" | grep -q "can only be used in a function"; then
        log_fail "Found illegal local keyword outside function: $output"
        return 1
    fi
    echo "$output" | grep -q -E "Failed to connect|Couldn't connect|Connection refused" || {
        log_fail "Expected curl connection error, but got: $output"
        return 1
    }
}

run_test test_es_delete_match_all_requires_explicit_gate
run_test test_es_delete_pair_cleanup_uses_published_layout
run_test test_es_cli_batch_delete_runtime_guard
