#!/bin/bash
# OWNER: D3-002
# Deterministic publication and process-death faults after the B2 transaction contract.
# Test stages are callbacks passed by name through test-lib.sh.
# shellcheck disable=SC1091,SC2317
source "$(dirname "$0")/../../lib/test-lib.sh"

declare -A BACKGROUND_GROUPS=()
REAL_MV="$(command -v mv)"
REAL_SYNC="$(command -v sync)"

register_background() {
    BACKGROUND_GROUPS["$1"]="$2"
}

unregister_background() {
    unset "BACKGROUND_GROUPS[$1]"
}

cleanup_background_processes() {
    local pid
    for pid in "${!BACKGROUND_GROUPS[@]}"; do
        if [[ "${BACKGROUND_GROUPS[$pid]}" == true ]]; then
            kill -- "-$pid" 2>/dev/null || true
        fi
        kill "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
        unset "BACKGROUND_GROUPS[$pid]"
    done
}
trap cleanup_background_processes EXIT

install_runtime() {
    local runtime="$1"
    mkdir -p "$runtime/app/bin" "$runtime/app/lib/scripts" "$runtime/wet" "$runtime/all"
    cp "$PROJECT_ROOT/src/main/dist/es-upsert.sh" "$runtime/es-upsert.sh"
    cp "$PROJECT_ROOT/src/main/dist/lib/scripts/runtime-lib.sh" \
        "$runtime/app/lib/scripts/runtime-lib.sh"
    cat > "$runtime/app/bin/es-cli" <<'FAKE_ES'
#!/bin/bash
command_name="${1:-}"
{
    printf '%q' "${1:-}"
    if [[ $# -gt 0 ]]; then
        shift
        printf ' %q' "$@"
    fi
    printf '\n'
} >> "$CALL_LOG"

if [[ "$command_name" == refresh ]]; then
    exit 0
fi

if [[ "$command_name" == batch-delete ]]; then
    jq -cn '{total:0,deleted:0,version_conflicts:0,timed_out:false,failures:[]}'
    exit 0
fi

if [[ "$command_name" != load-stream ]]; then
    exit 2
fi

if [[ -n "${BLOCK_READY:-}" ]]; then
    : > "$BLOCK_READY"
    while [[ ! -e "${BLOCK_RELEASE:-}" ]]; do
        sleep 0.02
    done
fi

json_mode=false
for argument in "$@"; do
    [[ "$argument" == --result-format=json ]] && json_mode=true
done

# This marker records the controlled loader's accepted success. Publication
# wrappers only fault after the successful child has returned to es-upsert.sh.
[[ -z "${ES_SUCCESS_MARKER:-}" ]] || : > "$ES_SUCCESS_MARKER"
if [[ "$json_mode" == true ]]; then
    jq -cn '{schema:"warc2es.processing/v1",status:"ok",exit_code:0,
      records_in:1,records_out:1,records_indexed:1,records_skipped:0,
      errors:0,elapsed_ms:1,error:null,
      metrics:{schema:"warc2es.metrics/v1",counters:{}}}'
fi
FAKE_ES
    chmod +x "$runtime/es-upsert.sh" "$runtime/app/bin/es-cli"
    : > "$runtime/calls"
}

install_fault_wrappers() {
    local wrapper_dir="$1"
    mkdir -p "$wrapper_dir"
    cat > "$wrapper_dir/mv" <<'MV_WRAPPER'
#!/bin/bash
: "${REAL_MV:?}"
if [[ -n "${ES_SUCCESS_MARKER:-}" && -e "$ES_SUCCESS_MARKER" ]]; then
    case "${FAULT_MODE:-}" in
        mv-fail)
            [[ -z "${FAULT_LOG:-}" ]] || printf 'mv-fail\n' >> "$FAULT_LOG"
            exit 73
            ;;
        mv-block)
            [[ -z "${FAULT_LOG:-}" ]] || printf 'mv-block\n' >> "$FAULT_LOG"
            [[ -z "${FAULT_READY:-}" ]] || : > "$FAULT_READY"
            while :; do sleep 0.02; done
            ;;
    esac
fi
exec "$REAL_MV" "$@"
MV_WRAPPER
    cat > "$wrapper_dir/sync" <<'SYNC_WRAPPER'
#!/bin/bash
: "${REAL_SYNC:?}"
if [[ "${FAULT_MODE:-}" == sync-fail && -n "${ES_SUCCESS_MARKER:-}" &&
      -e "$ES_SUCCESS_MARKER" && "${1:-}" == -f &&
      "${2:-}" == "${FAULT_PAIR_DIR:-}" ]]; then
    [[ -z "${FAULT_LOG:-}" ]] || printf 'sync-fail\n' >> "$FAULT_LOG"
    exit 74
fi
exec "$REAL_SYNC" "$@"
SYNC_WRAPPER
    chmod +x "$wrapper_dir/mv" "$wrapper_dir/sync"
}

make_wet() {
    local output="$1" url_id="$2" crawl_id="$3" payload="$4"
    local payload_length=${#payload}
    mkdir -p "$(dirname "$output")"
    printf 'WARC/1.0\r\nWARC-Type: conversion\r\nX-NAC-URL-ID: %s\r\nX-NAC-Crawl-ID: %s\r\nContent-Length: %s\r\n\r\n%s\r\n\r\n' \
        "$url_id" "$crawl_id" "$payload_length" "$payload" | gzip > "$output"
}

wet_sha() {
    sha256sum -- "$1" | awk '{print $1}'
}

seed_published_wet() {
    local runtime="$1" output_name="$2" url_id="$3" crawl_id="$4" payload="$5"
    local fixture="$runtime/$output_name" digest
    make_wet "$fixture" "$url_id" "$crawl_id" "$payload"
    digest="$(wet_sha "$fixture")"
    mkdir -p "$runtime/all/wet/$url_id/$crawl_id"
    cp -- "$fixture" "$runtime/all/wet/$url_id/$crawl_id/$digest.wet.gz"
    printf '%s\n' "$digest"
}

wait_for_file() {
    local file="$1" attempt
    for ((attempt = 0; attempt < 250; attempt++)); do
        [[ -e "$file" ]] && return 0
        sleep 0.02
    done
    return 1
}

wait_for_group_exit() {
    local group="$1" attempt
    for ((attempt = 0; attempt < 100; attempt++)); do
        kill -0 -- "-$group" 2>/dev/null || return 0
        sleep 0.02
    done
    return 1
}

assert_call_sequence() {
    local call_log="$1" repetitions="$2" index
    local -a calls=()
    mapfile -t calls < "$call_log"
    [[ ${#calls[@]} -eq $((repetitions * 3)) ]] || {
        log_fail "expected $((repetitions * 3)) Elasticsearch calls, got ${#calls[@]}"
        return 1
    }
    for ((index = 0; index < ${#calls[@]}; index += 3)); do
        [[ "${calls[index]}" == refresh\ * &&
           "${calls[index + 1]}" == batch-delete\ * &&
           "${calls[index + 2]}" == load-stream\ * ]] || {
            log_fail "transaction emitted calls outside refresh/delete/load order"
            return 1
        }
    done
}

assert_no_publication_temps() {
    local pair_dir="$1"
    [[ ! -d "$pair_dir" ]] ||
        [[ -z "$(find "$pair_dir" -mindepth 1 -maxdepth 1 -type f -name '.*.tmp.*' -print -quit)" ]] || {
            log_fail "publication temporary remained after handled failure"
            return 1
        }
}

assert_complete_temps_only() {
    local pair_dir="$1" selected_sha="$2" old_sha="$3"
    local entry base actual
    local temp_count=0
    while IFS= read -r -d '' entry; do
        base="$(basename "$entry")"
        if [[ "$base" == "$old_sha.wet.gz" ]]; then
            actual="$(wet_sha "$entry")"
            [[ "$actual" == "$old_sha" ]] || {
                log_fail "previously published WET changed during interrupted load"
                return 1
            }
        elif [[ "$base" =~ ^\.$selected_sha\.tmp\.[A-Za-z0-9]+$ ]]; then
            actual="$(wet_sha "$entry")"
            [[ "$actual" == "$selected_sha" ]] || {
                log_fail "interruption left an incomplete publication temporary"
                return 1
            }
            temp_count=$((temp_count + 1))
        else
            log_fail "interruption left an unexpected pair member: $base"
            return 1
        fi
    done < <(find "$pair_dir" -mindepth 1 -maxdepth 1 -type f -print0)
    [[ "$temp_count" -le 1 ]] || {
        log_fail "interruption left more than one transaction snapshot"
        return 1
    }
}

assert_partial_result() {
    local output="$1" publication_status="$2" expected_path_count="$3"
    jq -e --arg publication_status "$publication_status" \
        --argjson expected_path_count "$expected_path_count" '
      .schema == "warc2es.operator/v1" and .status == "partial" and
      .exit_code == 1 and .error.code == "publication_failed" and
      .processing.schema == "warc2es.processing/v1" and
      .processing.status == "ok" and
      .publication.status == $publication_status and
      (.publication.paths | length) == $expected_path_count and
      (has("es_state") | not)
    ' "$output" >/dev/null || {
        log_fail "publication fault did not emit a truthful partial operator result"
        return 1
    }
}

case_post_es_mv_failure() {
    local runtime="$TEST_OUTPUT_DIR/mv-failure"
    local input="$runtime/input.wet.gz" output="$runtime/result.json"
    local wrapper_dir="$runtime/wrappers" marker="$runtime/es-success"
    local fault_log="$runtime/faults" pair_dir="$runtime/all/wet/u/c"
    local selected_sha old_sha rc
    install_runtime "$runtime"
    old_sha="$(seed_published_wet "$runtime" old.wet.gz u c old)"
    make_wet "$input" u c selected
    selected_sha="$(wet_sha "$input")"
    install_fault_wrappers "$wrapper_dir"

    CALL_LOG="$runtime/calls" ES_SUCCESS_MARKER="$marker" FAULT_MODE=mv-fail \
      FAULT_LOG="$fault_log" REAL_MV="$REAL_MV" REAL_SYNC="$REAL_SYNC" \
      PATH="$wrapper_dir:$PATH" "$runtime/es-upsert.sh" "$input" \
      --url-id=u --crawl-id=c --result-format=json >"$output" 2>"$output.stderr"
    rc=$?

    assert_command_failure "$rc" "post-Elasticsearch mv failure succeeded" || return 1
    [[ -e "$marker" ]] || { log_fail "mv fault occurred before loader success"; return 1; }
    [[ "$(wet_sha "$input")" == "$selected_sha" ]] || {
        log_fail "mv failure changed the external source"
        return 1
    }
    assert_file_exists "$pair_dir/$old_sha.wet.gz" || return 1
    [[ ! -e "$pair_dir/$selected_sha.wet.gz" ]] || {
        log_fail "failed mv unexpectedly published the selected WET"
        return 1
    }
    assert_no_publication_temps "$pair_dir" || return 1
    [[ "$(wc -l < "$fault_log")" -eq 1 ]] || {
        log_fail "mv wrapper did not inject exactly one post-success fault"
        return 1
    }
    assert_partial_result "$output" unchanged 0 || return 1
    assert_call_sequence "$runtime/calls" 1
}

case_post_es_directory_sync_failure() {
    local runtime="$TEST_OUTPUT_DIR/sync-failure"
    local input="$runtime/input.wet.gz" output="$runtime/result.json"
    local wrapper_dir="$runtime/wrappers" marker="$runtime/es-success"
    local fault_log="$runtime/faults" pair_dir="$runtime/all/wet/u/c"
    local selected_sha old_sha rc relative_path
    install_runtime "$runtime"
    old_sha="$(seed_published_wet "$runtime" old.wet.gz u c old)"
    make_wet "$input" u c selected
    selected_sha="$(wet_sha "$input")"
    relative_path="all/wet/u/c/$selected_sha.wet.gz"
    install_fault_wrappers "$wrapper_dir"

    CALL_LOG="$runtime/calls" ES_SUCCESS_MARKER="$marker" FAULT_MODE=sync-fail \
      FAULT_LOG="$fault_log" FAULT_PAIR_DIR="$pair_dir" REAL_MV="$REAL_MV" \
      REAL_SYNC="$REAL_SYNC" PATH="$wrapper_dir:$PATH" \
      "$runtime/es-upsert.sh" "$input" --url-id=u --crawl-id=c \
      --result-format=json >"$output" 2>"$output.stderr"
    rc=$?

    assert_command_failure "$rc" "post-Elasticsearch directory sync failure succeeded" || return 1
    [[ -e "$marker" ]] || { log_fail "sync fault occurred before loader success"; return 1; }
    [[ "$(wet_sha "$input")" == "$selected_sha" ]] || {
        log_fail "sync failure changed the external source"
        return 1
    }
    assert_file_exists "$pair_dir/$old_sha.wet.gz" || return 1
    assert_file_exists "$pair_dir/$selected_sha.wet.gz" || return 1
    [[ "$(wet_sha "$pair_dir/$selected_sha.wet.gz")" == "$selected_sha" ]] || {
        log_fail "sync failure exposed an incomplete final artifact"
        return 1
    }
    assert_no_publication_temps "$pair_dir" || return 1
    [[ "$(wc -l < "$fault_log")" -eq 1 ]] || {
        log_fail "sync wrapper did not inject exactly one post-success directory fault"
        return 1
    }
    assert_partial_result "$output" published 1 || return 1
    jq -e --arg path "$relative_path" '.publication.paths == [$path]' "$output" >/dev/null || {
        log_fail "sync failure did not report the complete visible selected artifact"
        return 1
    }
    assert_call_sequence "$runtime/calls" 1
}

case_hard_death_then_retry() {
    local runtime="$TEST_OUTPUT_DIR/hard-death"
    local input="$runtime/input.wet.gz" output="$runtime/result.json"
    local retry_output="$runtime/retry.json" wrapper_dir="$runtime/wrappers"
    local marker="$runtime/es-success" ready="$runtime/mv-ready"
    local pair_dir="$runtime/all/wet/u/c" fault_log="$runtime/faults"
    local selected_sha old_sha pid rc temp retry_rc
    local -a temps=() members=()
    install_runtime "$runtime"
    old_sha="$(seed_published_wet "$runtime" old.wet.gz u c old)"
    make_wet "$input" u c selected
    selected_sha="$(wet_sha "$input")"
    install_fault_wrappers "$wrapper_dir"

    setsid env CALL_LOG="$runtime/calls" ES_SUCCESS_MARKER="$marker" \
      FAULT_MODE=mv-block FAULT_LOG="$fault_log" FAULT_READY="$ready" \
      REAL_MV="$REAL_MV" REAL_SYNC="$REAL_SYNC" PATH="$wrapper_dir:$PATH" \
      "$runtime/es-upsert.sh" "$input" --url-id=u --crawl-id=c \
      --result-format=json >"$output" 2>"$output.stderr" &
    pid=$!
    register_background "$pid" true
    wait_for_file "$ready" || {
        log_fail "publication mv did not block after loader success"
        return 1
    }
    [[ -e "$marker" ]] || { log_fail "hard-death fixture was not post-success"; return 1; }
    kill -KILL -- "-$pid"
    wait "$pid" 2>/dev/null
    rc=$?
    unregister_background "$pid"

    assert_command_failure "$rc" "SIGKILLed transaction reported success" || return 1
    [[ "$(wet_sha "$input")" == "$selected_sha" ]] || {
        log_fail "hard process death changed the external source"
        return 1
    }
    assert_file_exists "$pair_dir/$old_sha.wet.gz" || return 1
    [[ ! -e "$pair_dir/$selected_sha.wet.gz" ]] || {
        log_fail "blocked pre-publication transaction exposed a final artifact"
        return 1
    }
    mapfile -d '' -t temps < <(find "$pair_dir" -mindepth 1 -maxdepth 1 \
        -type f -name ".$selected_sha.tmp.*" -print0)
    [[ ${#temps[@]} -eq 1 ]] || {
        log_fail "SIGKILL did not leave exactly one unpublished transaction snapshot"
        return 1
    }
    temp="${temps[0]}"
    [[ "$(wet_sha "$temp")" == "$selected_sha" ]] || {
        log_fail "SIGKILL left an incomplete transaction snapshot"
        return 1
    }
    assert_call_sequence "$runtime/calls" 1 || return 1

    CALL_LOG="$runtime/calls" ES_SUCCESS_MARKER="$runtime/retry-success" \
      "$runtime/es-upsert.sh" "$input" --url-id=u --crawl-id=c \
      --result-format=json >"$retry_output" 2>"$retry_output.stderr"
    retry_rc=$?
    assert_command_success "$retry_rc" "retry after hard death failed" || return 1
    [[ "$(wet_sha "$input")" == "$selected_sha" ]] || {
        log_fail "retry changed the external source"
        return 1
    }
    assert_file_exists "$pair_dir/$selected_sha.wet.gz" || return 1
    [[ "$(wet_sha "$pair_dir/$selected_sha.wet.gz")" == "$selected_sha" ]] || {
        log_fail "retry published bytes under the wrong content address"
        return 1
    }
    mapfile -d '' -t members < <(find "$pair_dir" -mindepth 1 -maxdepth 1 -print0)
    [[ ${#members[@]} -eq 1 && "${members[0]}" == "$pair_dir/$selected_sha.wet.gz" ]] || {
        log_fail "retry did not reconstruct the exact selected publication set"
        return 1
    }
    jq -e --arg path "all/wet/u/c/$selected_sha.wet.gz" '
      .status == "ok" and .exit_code == 0 and .error == null and
      .publication == {status:"published",paths:[$path]} and
      (has("es_state") | not)
    ' "$retry_output" >/dev/null || {
        log_fail "retry did not emit a truthful successful publication result"
        return 1
    }
    assert_call_sequence "$runtime/calls" 2
}

run_load_signal_case() {
    local signal="$1" suffix="${1,,}"
    local runtime="$TEST_OUTPUT_DIR/signal-$suffix"
    local input="$runtime/input.wet.gz" output="$runtime/result.json"
    local ready="$runtime/load-ready" release="$runtime/never-release"
    local marker="$runtime/es-success" timeout_marker="$runtime/watchdog-fired"
    local pair_dir="$runtime/all/wet/u/c" selected_sha old_sha pid watchdog rc
    install_runtime "$runtime"
    old_sha="$(seed_published_wet "$runtime" old.wet.gz u c old)"
    make_wet "$input" u c selected
    selected_sha="$(wet_sha "$input")"

    # Bash starts asynchronous jobs with SIGINT ignored. Reset the inherited
    # dispositions before exec so this exercises the operator shell's signal
    # behavior instead of the test runner's background-job policy.
    # shellcheck disable=SC2016
    setsid perl -e '$SIG{INT}="DEFAULT"; $SIG{TERM}="DEFAULT"; exec @ARGV' \
      env CALL_LOG="$runtime/calls" BLOCK_READY="$ready" \
      BLOCK_RELEASE="$release" ES_SUCCESS_MARKER="$marker" \
      "$runtime/es-upsert.sh" "$input" --url-id=u --crawl-id=c \
      --result-format=json >"$output" 2>"$output.stderr" &
    pid=$!
    register_background "$pid" true
    wait_for_file "$ready" || {
        log_fail "$signal fixture did not reach the blocked loader"
        return 1
    }
    [[ ! -e "$marker" ]] || { log_fail "$signal fixture passed loader success"; return 1; }
    kill -s "$signal" -- "-$pid"

    (
        sleep 5
        if kill -0 "$pid" 2>/dev/null; then
            : > "$timeout_marker"
            kill -KILL -- "-$pid" 2>/dev/null || true
        fi
    ) &
    watchdog=$!
    register_background "$watchdog" false
    wait "$pid" 2>/dev/null
    rc=$?
    unregister_background "$pid"
    kill "$watchdog" 2>/dev/null || true
    wait "$watchdog" 2>/dev/null || true
    unregister_background "$watchdog"

    [[ ! -e "$timeout_marker" ]] || {
        log_fail "$signal did not terminate the complete invocation process group"
        return 1
    }
    assert_command_failure "$rc" "$signal-terminated transaction reported success" || return 1
    wait_for_group_exit "$pid" || {
        log_fail "$signal left a process alive in the invocation group"
        return 1
    }
    [[ ! -e "$marker" ]] || { log_fail "$signal fixture invented loader success"; return 1; }
    [[ "$(wet_sha "$input")" == "$selected_sha" ]] || {
        log_fail "$signal changed the external source"
        return 1
    }
    assert_file_exists "$pair_dir/$old_sha.wet.gz" || return 1
    [[ ! -e "$pair_dir/$selected_sha.wet.gz" ]] || {
        log_fail "$signal published a final artifact before loader success"
        return 1
    }
    assert_complete_temps_only "$pair_dir" "$selected_sha" "$old_sha" || return 1
    assert_call_sequence "$runtime/calls" 1
}

case_sigint_during_load() {
    run_load_signal_case INT
}

case_sigterm_during_load() {
    run_load_signal_case TERM
}

setup_test_env
run_stage "post-Elasticsearch publication mv failure is truthful" case_post_es_mv_failure || true
run_stage "post-Elasticsearch directory sync failure exposes complete files only" case_post_es_directory_sync_failure || true
run_stage "hard death before publication retries to the exact selected set" case_hard_death_then_retry || true
run_stage "SIGINT terminates the invocation while the loader runs" case_sigint_during_load || true
run_stage "SIGTERM terminates the invocation while the loader runs" case_sigterm_during_load || true
finish_stages
rc=$?
cleanup_background_processes
cleanup_test_env
exit "$rc"
