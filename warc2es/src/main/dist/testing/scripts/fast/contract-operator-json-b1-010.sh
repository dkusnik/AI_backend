#!/bin/bash
# OWNER: B1-010
# Shell JSON envelope, protocol reconciliation, dry-run, and ordered NDJSON contract.
source "$(dirname "$0")/../../lib/test-lib.sh"

install_runtime() {
    local runtime="$1"
    mkdir -p "$runtime/app/bin" "$runtime/app/lib/scripts" "$runtime/all" "$runtime/wet"
    cp "$PROJECT_ROOT/src/main/dist/es-upsert.sh" "$runtime/es-upsert.sh"
    cp "$PROJECT_ROOT/src/main/dist/es-upsert-all.sh" "$runtime/es-upsert-all.sh"
    cp "$PROJECT_ROOT/src/main/dist/lib/scripts/runtime-lib.sh" \
        "$runtime/app/lib/scripts/runtime-lib.sh"
cat > "$runtime/app/bin/es-cli" <<'FAKE_ES'
#!/bin/bash
printf '%s\n' "$*" >> "$CALL_LOG"
if [[ "${1:-}" == "refresh" ]]; then
    exit 0
fi
if [[ "${1:-}" == "batch-delete" ]]; then
    jq -cn '{total:0,deleted:0,version_conflicts:0,timed_out:false,failures:[]}'
    exit 0
fi
emit_result() {
    local status="$1" exit_code="$2"
    jq -cn --arg status "$status" --argjson exit_code "$exit_code" \
      '{schema:"warc2es.processing/v1",status:$status,exit_code:$exit_code,
        records_in:1,records_out:1,records_indexed:1,records_skipped:0,
        errors:(if $status == "error" then 1 else 0 end),elapsed_ms:1,
        error:(if $status == "error" then {code:"processing_failed",message:"fixture"} else null end),
        metrics:{schema:"warc2es.metrics/v1",counters:{}}}'
}

case "${FAKE_MODE:-ok}" in
    ok) emit_result ok 0 ;;
    error) emit_result error 7; exit 7 ;;
    missing) exit 5 ;;
    invalid) printf 'not-json\n' ;;
    multiple) emit_result ok 0; emit_result ok 0 ;;
    extra) emit_result ok 0; printf 'extra\n' ;;
    nonzero_ok) emit_result ok 0; exit 5 ;;
    zero_error) emit_result error 0 ;;
    exit_mismatch) emit_result error 7; exit 8 ;;
    mixed)
        if [[ "$*" == *'/z/'* ]]; then
            emit_result error 7
            exit 7
        fi
        emit_result ok 0
        ;;
    always_error) emit_result error 7; exit 7 ;;
esac
FAKE_ES
    chmod +x "$runtime/es-upsert.sh" "$runtime/es-upsert-all.sh" "$runtime/app/bin/es-cli"
}

make_wet() {
    local output="$1" url_id="$2" crawl_id="$3" payload="${4:-payload}"
    local payload_length=${#payload}
    printf 'WARC/1.0\r\nWARC-Type: conversion\r\nX-NAC-URL-ID: %s\r\nX-NAC-Crawl-ID: %s\r\nContent-Length: %s\r\n\r\n%s\r\n\r\n' \
        "$url_id" "$crawl_id" "$payload_length" "$payload" | gzip > "$output"
}

run_upsert_json() {
    local runtime="$1" mode="$2" output="$3"
    shift 3
    set +e
    CALL_LOG="$runtime/calls" FAKE_MODE="$mode" \
        "$runtime/es-upsert.sh" "$@" >"$output" \
        2>"$output.stderr"
    COMMAND_RC=$?
    set -e
}

case_one_aggregate_transaction() {
    local runtime="$TEST_OUTPUT_DIR/direct"
    local input="$runtime/input"
    local output="$runtime/result.json"
    install_runtime "$runtime"
    mkdir -p "$input/nested"
    make_wet "$input/b.wet.gz" u c b
    make_wet "$input/nested/a.wet.gz" u c a
    : > "$runtime/calls"

    run_upsert_json "$runtime" ok "$output" "$input" --url-id=u --crawl-id=c
    assert_command_success "$COMMAND_RC" "direct JSON upsert failed" || return 1
    [[ "$(grep -c '^load-stream ' "$runtime/calls")" -eq 1 ]] || {
        log_fail "directory invocation launched more than one Java process"
        return 1
    }
    [[ "$(grep -c '^batch-delete ' "$runtime/calls")" -eq 1 ]] || {
        log_fail "directory invocation did not delete the pair exactly once"
        return 1
    }
    [[ "$(wc -l < "$output")" -eq 1 ]] || {
        log_fail "direct JSON invocation did not emit exactly one line"
        return 1
    }
    jq -e '
      .schema == "warc2es.operator/v1" and .kind == "invocation" and
      .command == "es-upsert" and .status == "ok" and .exit_code == 0 and
      (.processing | type) == "object" and
      [.inputs[].path] == ["b.wet.gz","nested/a.wet.gz"] and
      .outputs == [] and .error == null
    ' "$output" >/dev/null || return 1
}

case_es_cli_preserves_first_batch_input() {
    local runtime="$TEST_OUTPUT_DIR/es-cli-batch"
    local capture="$runtime/args.nul"
    local first="$runtime/a.wet.gz"
    local second="$runtime/b.wet.gz"
    local arg
    local -a args=()
    local -a files=()
    mkdir -p "$runtime/app/bin" "$runtime/app/lib/scripts"
    cp "$PROJECT_ROOT/src/main/dist/bin/es-cli" "$runtime/app/bin/es-cli"
    cat > "$runtime/app/lib/scripts/pipeline-lib" <<'FAKE_PIPELINE'
run_pipeline() {
    printf '%s\0' "$@" > "$CAPTURE"
}
run_pipeline_global() {
    run_pipeline "$@"
}
FAKE_PIPELINE
    chmod +x "$runtime/app/bin/es-cli"
    : > "$first"
    : > "$second"

    CAPTURE="$capture" ES_CLI_CONF="$runtime/no-cluster-config" \
        "$runtime/app/bin/es-cli" load-stream "$first" stream "$second"
    mapfile -d '' -t args < "$capture"
    for arg in "${args[@]}"; do
        [[ "$arg" == *.wet.gz ]] && files+=("$arg")
    done
    [[ ${#files[@]} -eq 3 && "${files[0]}" == "$first" && \
       "${files[1]}" == "$first" && "${files[2]}" == "$second" ]] || {
        log_fail "es-cli did not preserve positional 0 in the complete parser remainder"
        return 1
    }
}

case_reconcile_status_and_exit() {
    local runtime="$TEST_OUTPUT_DIR/reconcile"
    local input="$runtime/input.wet.gz"
    local output="$runtime/result.json"
    local mode
    install_runtime "$runtime"
    make_wet "$input" u c

    for mode in nonzero_ok zero_error exit_mismatch; do
        : > "$runtime/calls"
        run_upsert_json "$runtime" "$mode" "$output" "$input" --url-id=u --crawl-id=c
        assert_command_failure "$COMMAND_RC" "$mode disagreement succeeded" || return 1
        jq -e '.status == "error" and .exit_code != 0 and
               .error.code == "processing_protocol_error" and
               .publication == {status:"skipped",paths:[]}' \
            "$output" >/dev/null || return 1
    done
}

case_missing_and_invalid_results() {
    local runtime="$TEST_OUTPUT_DIR/fallback"
    local input="$runtime/input.wet.gz"
    local output="$runtime/result.json"
    local mode
    install_runtime "$runtime"
    make_wet "$input" u c

    run_upsert_json "$runtime" missing "$output" "$input" --url-id=u --crawl-id=c
    assert_command_failure "$COMMAND_RC" "missing processing result succeeded" || return 1
    jq -e '.error.code == "processing_result_missing" and
           .processing.result_missing == true and .processing.exit_code == 5' \
        "$output" >/dev/null || return 1

    for mode in invalid multiple extra; do
        run_upsert_json "$runtime" "$mode" "$output" "$input" --url-id=u --crawl-id=c
        assert_command_failure "$COMMAND_RC" "$mode processing result succeeded" || return 1
        jq -e '.error.code == "processing_result_invalid" and
               .processing.result_invalid == true' "$output" >/dev/null || return 1
    done
}

case_valid_processing_failure_is_preserved() {
    local runtime="$TEST_OUTPUT_DIR/processing-failure"
    local input="$runtime/input.wet.gz"
    local output="$runtime/result.json"
    install_runtime "$runtime"
    make_wet "$input" u c

    run_upsert_json "$runtime" error "$output" "$input" --url-id=u --crawl-id=c
    [[ "$COMMAND_RC" -eq 7 ]] || {
        log_fail "valid Java failure exit was not preserved: $COMMAND_RC"
        return 1
    }
    jq -e '.status == "error" and .exit_code == 7 and
           .error.code == "processing_failed" and
           .processing.status == "error" and .processing.exit_code == 7 and
           (.processing | has("result_missing") | not) and
           .publication == {status:"skipped",paths:[]}' \
        "$output" >/dev/null || return 1
}

case_dry_run_has_zero_calls() {
    local runtime="$TEST_OUTPUT_DIR/dry-run"
    local input="$runtime/input.wet.gz"
    local output="$runtime/result.json"
    install_runtime "$runtime"
    : > "$input"
    : > "$runtime/calls"

    run_upsert_json "$runtime" ok "$output" "$input" --url-id=u --crawl-id=c --dry-run
    assert_command_success "$COMMAND_RC" "JSON dry-run failed" || return 1
    [[ ! -s "$runtime/calls" ]] || {
        log_fail "dry-run invoked es-cli"
        return 1
    }
    jq -e '.status == "dry_run" and .exit_code == 0 and .processing == null and
           .publication == {status:"skipped",paths:[]}' "$output" >/dev/null || return 1
}

case_direct_json_and_replay_ndjson() {
    local runtime="$TEST_OUTPUT_DIR/replay"
    local output="$runtime/replay.jsonl"
    local empty="$runtime/empty.jsonl"
    local all_failed="$runtime/all-failed.jsonl"
    local invalid="$runtime/invalid-delegate.jsonl"
    local rc
    install_runtime "$runtime"
    : > "$runtime/calls"

    "$runtime/es-upsert-all.sh" >"$empty" 2>"$empty.stderr"
    jq -se 'length == 1 and .[0].kind == "summary" and
            .[0].status == "ok" and .[0].total == 0 and
            .[0].succeeded == 0 and .[0].failed == 0' "$empty" >/dev/null || return 1

    local a_sha z_sha
    mkdir -p "$runtime/all/wet/z/c" "$runtime/all/wet/a/c"
    make_wet "$runtime/all/wet/a/c/pending.wet.gz" a c
    make_wet "$runtime/all/wet/z/c/pending.wet.gz" z c
    a_sha=$(sha256sum -- "$runtime/all/wet/a/c/pending.wet.gz")
    a_sha=${a_sha%% *}
    z_sha=$(sha256sum -- "$runtime/all/wet/z/c/pending.wet.gz")
    z_sha=${z_sha%% *}
    mv -- "$runtime/all/wet/a/c/pending.wet.gz" \
        "$runtime/all/wet/a/c/$a_sha.wet.gz"
    mv -- "$runtime/all/wet/z/c/pending.wet.gz" \
        "$runtime/all/wet/z/c/$z_sha.wet.gz"
    set +e
    CALL_LOG="$runtime/calls" FAKE_MODE=mixed "$runtime/es-upsert-all.sh" \
        >"$output" 2>"$output.stderr"
    rc=$?
    set -e
    assert_command_failure "$rc" "mixed replay returned success" || return 1
    jq -se '
      length == 3 and
      .[0].kind == "invocation" and
      .[0].publication.paths[0] == ("all/wet/a/c/" + $a_sha + ".wet.gz") and
      .[1].kind == "invocation" and .[1].publication.status == "skipped" and
      .[2].kind == "summary" and .[2].status == "partial" and .[2].exit_code == 1 and
      .[2].total == 2 and .[2].succeeded == 1 and .[2].failed == 1
    ' --arg a_sha "$a_sha" "$output" >/dev/null || return 1

    set +e
    CALL_LOG="$runtime/calls" FAKE_MODE=always_error "$runtime/es-upsert-all.sh" \
        >"$all_failed" 2>"$all_failed.stderr"
    rc=$?
    set -e
    assert_command_failure "$rc" "all-failed replay returned success" || return 1
    jq -se '.[-1].kind == "summary" and .[-1].status == "error" and
            .[-1].total == 2 and .[-1].succeeded == 0 and .[-1].failed == 2' \
        "$all_failed" >/dev/null || return 1

    cat > "$runtime/es-upsert.sh" <<'INVALID_DELEGATE'
#!/bin/bash
printf '%s\n' '{"schema":"warc2es.operator/v1","kind":"invocation","command":"es-upsert","status":"ok","exit_code":"zero","mode":"archive-replay","inputs":[],"outputs":[],"publication":{"status":"unchanged","paths":[]},"processing":null,"error":null}'
INVALID_DELEGATE
    chmod +x "$runtime/es-upsert.sh"
    set +e
    "$runtime/es-upsert-all.sh" >"$invalid" 2>"$invalid.stderr"
    rc=$?
    set -e
    assert_command_failure "$rc" "invalid delegated results returned success" || return 1
    jq -se '
      length == 3 and
      (.[0:2] | all(.kind == "invocation" and .status == "error" and
                    .error.code == "processing_result_invalid")) and
      .[2].kind == "summary" and .[2].status == "error" and
      .[2].total == 2 and .[2].succeeded == 0 and .[2].failed == 2
    ' "$invalid" >/dev/null || return 1
}

case_path_escaping_and_utf8_refusal() {
    local runtime="$TEST_OUTPUT_DIR/paths"
    local input="$runtime/input"
    local output="$runtime/result.json"
    local weird=$'tab\tline\nquote"slash\\.wet.gz'
    install_runtime "$runtime"
    mkdir -p "$input"
    : > "$input/$weird"
    : > "$runtime/calls"

    run_upsert_json "$runtime" ok "$output" "$input" --url-id=u --crawl-id=c --dry-run
    assert_command_success "$COMMAND_RC" "escaped UTF-8 path dry-run failed" || return 1
    jq -e --arg path "$weird" '.inputs == [{input_index:0,path:$path}]' \
        "$output" >/dev/null || return 1

    # This filesystem refuses creation of invalid UTF-8 names, so exercise the
    # same validator used by discovery directly.
    # shellcheck source=/dev/null
    source "$PROJECT_ROOT/src/main/dist/lib/scripts/runtime-lib.sh"
    if _runtime_path_is_utf8 $'bad-\xff.wet.gz'; then
        log_fail "invalid UTF-8 path bytes were accepted"
        return 1
    fi
}

setup_test_env
run_stage "one direct invocation is one aggregate transaction" case_one_aggregate_transaction || true
run_stage "es-cli keeps the first WET in multi-input dispatch" case_es_cli_preserves_first_batch_input || true
run_stage "processing status and process exit are reconciled" case_reconcile_status_and_exit || true
run_stage "missing and invalid Java results get discriminated fallbacks" case_missing_and_invalid_results || true
run_stage "valid Java failure is embedded unchanged" case_valid_processing_failure_is_preserved || true
run_stage "dry-run respects output mode and makes zero calls" case_dry_run_has_zero_calls || true
run_stage "direct JSON and ordered replay summary NDJSON" case_direct_json_and_replay_ndjson || true
run_stage "path escaping and invalid UTF-8 policy" case_path_escaping_and_utf8_refusal || true
finish_stages
rc=$?
cleanup_test_env
exit "$rc"
