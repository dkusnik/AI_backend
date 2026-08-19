#!/bin/bash
# Enforce release path/deprecation policy for user-facing commands and docs.
# Task-ID: T-246
source "$(dirname "$0")/../../lib/test-lib.sh"

test_policy_path_guard() {
    local rc=0
    local help_text
    local cli_test="$BIN_DIR/test-cli"

    assert_file_exists "$WARC_CLI" || return 1
    assert_file_exists "$ES_CLI" || return 1
    assert_file_exists "$cli_test" || return 1

    help_text=$("$WARC_CLI" --help 2>&1)
    if echo "$help_text" | grep -qiE 'deprecated|depreciated|regen-recompress'; then
        log_fail "warc-cli help contains deprecated compatibility text"
        rc=1
    fi

    help_text=$("$ES_CLI" 2>&1 || true)
    if echo "$help_text" | grep -qiE 'deprecated|depreciated|regen-recompress'; then
        log_fail "es-cli output contains deprecated compatibility text"
        rc=1
    fi

    help_text=$("$cli_test" --help 2>&1 || true)
    if echo "$help_text" | grep -qiE 'deprecated|depreciated|regen-recompress'; then
        log_fail "test-cli help contains deprecated compatibility text"
        rc=1
    fi

    if rg -n '/home/[^[:space:]"'\''`]+|/Users/[^[:space:]"'\''`]+' \
        README.md TODO.md src/main/dist/bin/warc-cli src/main/dist/bin/es-cli src/main/dist/bin/test-cli src/main/dist/testing/lib/test-lib.sh >/dev/null; then
        log_fail "Hardcoded absolute user paths found in active docs/CLI/test harness files"
        rc=1
    fi

    if rg -n '(^|[[:space:]])\./dist/bin/|warc2es/dist/bin|src/main/dist/testing/test-cli|/warc-workspace/pipeline' README.md TODO.md >/dev/null; then
        log_fail "Forbidden user-facing command path references (dist/pipeline legacy) found in docs"
        rc=1
    fi

    if rg -n 'tmp/testing|dist/testing/tmp|\$\{?PROJECT_ROOT\}?/testing(/|$)' \
        src/main/dist/bin/test-cli src/main/dist/testing \
        --glob '!scripts/fast/s0-policy-path-guard.sh' >/dev/null; then
        log_fail "Legacy test output roots found; expected target/testing only"
        rc=1
    fi

    local leaked
    for leaked in "$PROJECT_ROOT/testing" "$PROJECT_ROOT/src/main/testing"; do
        if [[ -e "$leaked" ]]; then
            log_fail "Generated test state leaked outside target/: $leaked"
            rc=1
        fi
    done

    if [[ -d "$PROJECT_ROOT/dist" ]]; then
        log_fail "Legacy root dist/ directory exists; expected target/dist/ and out/ only"
        rc=1
    fi

    return $rc
}

run_test test_policy_path_guard
