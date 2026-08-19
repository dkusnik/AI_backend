#!/bin/bash
# OWNER: B1-009
# Recursive canonical NUL-safe bytewise traversal over the normative fixture.
source "$(dirname "$0")/../../lib/test-lib.sh"

fixture="$TEST_OUTPUT_DIR/fixture"
actual="$TEST_OUTPUT_DIR/actual.hex"
expected="$PROJECT_ROOT/src/main/dist/testing/fixtures/traversal-order/expected-order.hex"

require_traversal() {
    # shellcheck source=/dev/null
    source "$PROJECT_ROOT/src/main/dist/lib/scripts/runtime-lib.sh"
    declare -F runtime_find_data_files >/dev/null || {
        log_fail "runtime_find_data_files is not implemented"
        return 1
    }
}

case_recursive_and_escape_safe() {
    require_traversal || return 1
    local output="$TEST_OUTPUT_DIR/recursive.nul"
    runtime_find_data_files wet "$fixture/valid-root" >"$output" || return 1
    tr '\0' '\n' <"$output" | grep -Fq '/nested/02.wet.gz' || return 1
    if runtime_find_data_files wet "$fixture/escape-root" >"$TEST_OUTPUT_DIR/escape.nul" 2>/dev/null; then
        log_fail "symlink escape was accepted"
        return 1
    fi
}

case_bytewise_order() {
    require_traversal || return 1
    : >"$actual"
    local path relative
    while IFS= read -r -d '' path; do
        relative="${path#"$fixture/valid-root/"}"
        printf '%s' "$relative" | od -An -tx1 -v | tr -d ' \n' >>"$actual"
        printf '\n' >>"$actual"
    done < <(runtime_find_data_files wet "$fixture/valid-root")
    cmp -s "$expected" "$actual" || { log_fail "traversal byte order differs"; return 1; }
}

case_extension_set() {
    require_traversal || return 1
    local path count=0
    while IFS= read -r -d '' path; do
        [[ "$path" == *.wet.gz ]] || { log_fail "unsupported extension selected: $path"; return 1; }
        count=$((count + 1))
    done < <(runtime_find_data_files wet "$fixture/valid-root")
    [[ "$count" -eq 8 ]] || { log_fail "expected 8 canonical .wet.gz artifacts, got $count"; return 1; }
}

case_warc_extensions_and_utf8() {
    require_traversal || return 1
    local root="$TEST_OUTPUT_DIR/warc-root"
    local unsupported="$TEST_OUTPUT_DIR/unsupported.wet"
    local invalid
    local -a files=()

    mkdir -p "$root/nested"
    : > "$root/A.warc"
    : > "$root/nested/b.warc.gz"
    : > "$root/ignored.warc.zst"
    : > "$root/ignored.WARC"
    runtime_find_data_files warc "$root" files || return 1
    [[ ${#files[@]} -eq 2 && "${files[0]}" == "$root/A.warc" && \
       "${files[1]}" == "$root/nested/b.warc.gz" ]] || {
        log_fail "WARC extension set or byte order differs"
        return 1
    }

    : > "$unsupported"
    if runtime_find_data_files wet "$unsupported" >/dev/null 2>&1; then
        log_fail "Explicit unsupported WET file was accepted"
        return 1
    fi

    invalid=$'bad-\xff.wet.gz'
    if _runtime_path_is_utf8 "$invalid"; then
        log_fail "Invalid UTF-8 path component was accepted"
        return 1
    fi
}

case_warc2wet_root_order_and_dedup() {
    local runtime="$TEST_OUTPUT_DIR/order-runtime"
    local scripts="$runtime/app/lib/scripts"
    local wrapper="$runtime/warc2wet.sh"
    local root_one="$runtime/root-one"
    local root_two="$runtime/root-two"
    local capture="$runtime/args.nul"
    local output rc arg
    local -a args=()
    local -a inputs=()

    mkdir -p "$scripts" "$runtime/app/var/db" "$root_one/nested" "$root_two"
    cp "$PROJECT_ROOT/src/main/dist/warc2wet.sh" "$wrapper"
    cp "$PROJECT_ROOT/src/main/dist/lib/scripts/runtime-lib.sh" "$scripts/runtime-lib.sh"
    cat > "$scripts/pipeline-lib" <<'FAKE_PIPELINE'
run_pipeline() {
    printf '%s\0' "$@" > "$CAPTURE"
}
FAKE_PIPELINE

    for arg in "$root_one/nested/d.warc" "$root_one/z.warc" "$root_two/a.warc"; do
        printf 'WARC/1.0\r\nWARC-Date: 2026-01-02T03:04:05Z\r\n\r\n' > "$arg"
    done

    output=$(CAPTURE="$capture" bash "$wrapper" --url-id=Site --crawl-id=Crawl \
        --result-format=human "$root_one" "$root_two" "$root_one/nested" 2>&1)
    rc=$?
    assert_command_success "$rc" "warc2wet traversal integration failed: $output" || return 1

    mapfile -d '' -t args < "$capture"
    for arg in "${args[@]}"; do
        [[ "$arg" == *.warc || "$arg" == *.warc.gz ]] && inputs+=("$arg")
    done
    [[ ${#inputs[@]} -eq 3 && \
       "${inputs[0]}" == "$root_one/nested/d.warc" && \
       "${inputs[1]}" == "$root_one/z.warc" && \
       "${inputs[2]}" == "$root_two/a.warc" ]] || {
        log_fail "warc2wet did not preserve per-root order and first-occurrence deduplication"
        return 1
    }
}

setup_test_env
"$PROJECT_ROOT/src/main/dist/testing/fixtures/traversal-order/materialize.sh" "$fixture"
run_stage "recursive canonical traversal and escape refusal" case_recursive_and_escape_safe || true
run_stage "LC_ALL=C relative-byte order" case_bytewise_order || true
run_stage "case-sensitive .wet.gz extension set" case_extension_set || true
run_stage "WARC suffixes and UTF-8 validation" case_warc_extensions_and_utf8 || true
run_stage "warc2wet root order and canonical deduplication" case_warc2wet_root_order_and_dedup || true
finish_stages
rc=$?
cleanup_test_env
exit "$rc"
