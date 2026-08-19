#!/bin/bash
# OWNER: C2-002
# Sibling-temporary replacement and partial operator results.
source "$(dirname "$0")/../../lib/test-lib.sh"

install_runtime() {
    local runtime="$1"
    mkdir -p "$runtime/app/lib/scripts" "$runtime/app/var/db"
    cp "$PROJECT_ROOT/src/main/dist/warc2wet.sh" "$runtime/warc2wet.sh"
    cp "$PROJECT_ROOT/src/main/dist/lib/scripts/runtime-lib.sh" \
        "$runtime/app/lib/scripts/runtime-lib.sh"
    cat > "$runtime/app/lib/scripts/pipeline-lib" <<'FAKE_PIPELINE'
run_pipeline() {
    local arg output="" output_template="" report="" per_day=false json=false force=false
    printf '%s\0' "$@" > "$ARG_LOG"
    for arg in "$@"; do
        case "$arg" in
            --output.file=*) output="${arg#*=}" ;;
            --consumer.codec.output-name-template=*) output_template="${arg#*=}" ;;
            --consumer.codec.publication-report=*) report="${arg#*=}" ;;
            --consumer.codec.output-format=multi-warc) per_day=true ;;
            --result-format=json) json=true ;;
            --force) force=true ;;
        esac
    done
    [[ -n "$report" ]] || return 90

    if [[ "$per_day" == false ]]; then
        mkdir -p "$(dirname "$output")"
        if [[ -e "$output" && "$force" == false ]]; then
            jq -cn '{schema:"warc2es.output-publication/v1",status:"discarded",planned:0,published:[]}' \
                > "$report"
            [[ "$json" == false ]] || processing_error
            return 7
        fi
        local temporary
        temporary="$(mktemp "$(dirname "$output")/.$(basename "$output").XXXXXX.tmp")"
        printf 'new\n' > "$temporary"
        mv -f -- "$temporary" "$output"
        jq -cn --arg path "$output" \
            '{schema:"warc2es.output-publication/v1",status:"published",planned:1,published:[$path]}' \
            > "$report"
        [[ "$json" == false ]] || processing_ok
        return 0
    fi

    mkdir -p "$output"
    local first_name="${output_template/\{source\}/20260101}"
    local second_name="${output_template/\{source\}/20260102}"
    local first="$output/$first_name"
    local second="$output/$second_name"
    local first_tmp="$output/.$first_name.fixture.tmp"
    local second_tmp="$output/.$second_name.fixture.tmp"
    printf 'first\n' > "$first_tmp"
    printf 'second\n' > "$second_tmp"
    mv -- "$first_tmp" "$first"
    rm -f -- "$second_tmp"
    jq -cn --arg path "$first" \
        '{schema:"warc2es.output-publication/v1",status:"partial",planned:2,published:[$path]}' \
        > "$report"
    [[ "$json" == false ]] || processing_error
    return 7
}

processing_ok() {
    jq -cn '{schema:"warc2es.processing/v1",status:"ok",exit_code:0,
      records_in:1,records_out:1,records_indexed:null,records_skipped:0,
      errors:0,elapsed_ms:1,error:null,metrics:{schema:"warc2es.metrics/v1",counters:{}}}'
}

processing_error() {
    jq -cn '{schema:"warc2es.processing/v1",status:"error",exit_code:7,
      records_in:1,records_out:1,records_indexed:null,records_skipped:0,
      errors:1,elapsed_ms:1,error:{code:"after_check_failed",message:"publication failed"},
      metrics:{schema:"warc2es.metrics/v1",counters:{}}}'
}
FAKE_PIPELINE
    chmod +x "$runtime/warc2wet.sh"
}

write_warc() {
    printf 'WARC/1.0\r\nWARC-Date: 2026-01-02T03:04:05Z\r\n\r\n' > "$1"
}

run_wrapper() {
    local runtime="$1" result="$2"
    shift 2
    set +e
    ARG_LOG="$runtime/args.nul" "$runtime/warc2wet.sh" "$@" > "$result" 2> "$result.stderr"
    COMMAND_RC=$?
    set -e
}

case_derived_target_replacement() {
    local runtime="$TEST_OUTPUT_DIR/force"
    local input="$runtime/input.warc"
    local result="$runtime/result.json"
    local target="$runtime/wet/u/c/input.wet.gz"
    install_runtime "$runtime"
    write_warc "$input"
    mkdir -p "$(dirname "$target")"
    printf 'old\n' > "$target"

    run_wrapper "$runtime" "$result" --url-id=u --crawl-id=c --result-format=json "$input"
    assert_command_success "$COMMAND_RC" "derived staging replacement failed" || return 1
    [[ "$(cat "$target")" == new ]] || return 1
    jq -e '.status == "ok" and .outputs == ["wet/u/c/input.wet.gz"] and .publication == null' \
        "$result" >/dev/null || return 1
    # --force is private Java wiring now, not a public wrapper option.
    tr '\0' '\n' < "$runtime/args.nul" | grep -Fxq -- '--force' || return 1
    tr '\0' '\n' < "$runtime/args.nul" | grep -Fxq -- \
        '--consumer.codec.output-format=wet' || return 1
    tr '\0' '\n' < "$runtime/args.nul" | grep -Fxq -- \
        '--consumer.codec.cdx-sidecar=false' || return 1
    [[ ! -e "$runtime/wet/u/c/input.cdxj" ]] || return 1

    run_wrapper "$runtime" "$result" --url-id=u --crawl-id=c --force "$input"
    assert_command_failure "$COMMAND_RC" "retired public --force option succeeded" || return 1
}

case_partial_publication_reports_exact_completed_prefix() {
    local runtime="$TEST_OUTPUT_DIR/partial"
    local input="$runtime/input.warc"
    local result="$runtime/result.json"
    install_runtime "$runtime"
    write_warc "$input"

    run_wrapper "$runtime" "$result" --url-id=u --crawl-id=c --per-day \
        --result-format=json "$input"
    [[ "$COMMAND_RC" -eq 7 ]] || {
        log_fail "partial publication did not preserve Java's nonzero exit"
        return 1
    }
    jq -e '.status == "partial" and .exit_code == 7 and
           .outputs == ["wet/u/c/input-20260101.wet.gz"] and
           .publication == null and .processing.status == "error"' "$result" >/dev/null || return 1
    [[ -f "$runtime/wet/u/c/input-20260101.wet.gz" ]] || return 1
    [[ ! -e "$runtime/wet/u/c/input-20260102.wet.gz" ]] || return 1
    if find "$runtime/wet/u/c" -maxdepth 1 -name '*.tmp' -print -quit | grep -q .; then
        log_fail "partial publication left a sibling temporary"
        return 1
    fi
}

setup_test_env
run_stage "derived target is replaced only after successful processing" case_derived_target_replacement || true
run_stage "partial publication reports only the completed sorted prefix" case_partial_publication_reports_exact_completed_prefix || true
finish_stages
rc=$?
cleanup_test_env
exit "$rc"
