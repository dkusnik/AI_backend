#!/bin/bash
# OWNER: B1-011
# Required provenance, one-JVM extraction, stable staging, and operator JSON.
source "$(dirname "$0")/../../lib/test-lib.sh"

install_runtime() {
    local runtime="$1"
    mkdir -p "$runtime/app/lib/scripts" "$runtime/app/var/db"
    cp "$PROJECT_ROOT/src/main/dist/warc2wet.sh" "$runtime/warc2wet.sh"
    cp "$PROJECT_ROOT/src/main/dist/lib/scripts/runtime-lib.sh" \
        "$runtime/app/lib/scripts/runtime-lib.sh"
    cat > "$runtime/app/lib/scripts/pipeline-lib" <<'FAKE_PIPELINE'
run_pipeline() {
    local arg output="" output_template="" publication_report="" per_day=false json=false
    printf 'call\n' >> "$CALL_LOG"
    printf '%s\0' "$@" > "$ARG_LOG"
    for arg in "$@"; do
        case "$arg" in
            --output.file=*) output="${arg#*=}" ;;
            --consumer.codec.output-name-template=*) output_template="${arg#*=}" ;;
            --consumer.codec.publication-report=*) publication_report="${arg#*=}" ;;
            --consumer.codec.output-format=multi-warc) per_day=true ;;
            --result-format=json) json=true ;;
        esac
    done

    if [[ "${FAKE_MODE:-ok}" == error ]]; then
        if [[ "$json" == true ]]; then
            jq -cn '{schema:"warc2es.processing/v1",status:"error",exit_code:7,
              records_in:2,records_out:0,records_indexed:null,records_skipped:2,
              errors:1,elapsed_ms:2,error:{code:"fixture",message:"failed"},
              metrics:{schema:"warc2es.metrics/v1",counters:{}}}'
        fi
        return 7
    fi

    if [[ "$per_day" == true ]]; then
        local first_name="${output_template/\{source\}/20260101}"
        local second_name="${output_template/\{source\}/20260102}"
        mkdir -p "$output"
        : > "$output/$second_name"
        : > "$output/$first_name"
        jq -cn \
          --arg first "$output/$first_name" \
          --arg second "$output/$second_name" '
          {schema:"warc2es.output-publication/v1",status:"published",planned:2,
           published:[$first,$second],
           output_stats:{count:2,content_bytes:12,
             mime_types:{"application/pdf":1,"text/html":1},languages:{pl:1},
             missing_language:1,missing_mimetype:0,
             date_min:"2026-01-01T01:02:03Z",date_max:"2026-01-02T03:04:05Z",
             artifacts:[
               {path:$first,count:1,content_bytes:4,mime_types:{"text/html":1},
                languages:{pl:1},missing_language:0,missing_mimetype:0,
                date_min:"2026-01-01T01:02:03Z",date_max:"2026-01-01T01:02:03Z"},
               {path:$second,count:1,content_bytes:8,mime_types:{"application/pdf":1},
                languages:{},missing_language:1,missing_mimetype:0,
                date_min:"2026-01-02T03:04:05Z",date_max:"2026-01-02T03:04:05Z"}
             ]}}' > "$publication_report"
    else
        mkdir -p "$(dirname "$output")"
        : > "$output"
        jq -cn --arg path "$output" '
          {schema:"warc2es.output-publication/v1",status:"published",planned:1,
           published:[$path],
           output_stats:{count:2,content_bytes:12,
             mime_types:{"application/pdf":1,"text/html":1},languages:{pl:1},
             missing_language:1,missing_mimetype:0,
             date_min:"2026-01-01T01:02:03Z",date_max:"2026-01-02T03:04:05Z",
             artifacts:[
               {path:$path,count:2,content_bytes:12,
                mime_types:{"application/pdf":1,"text/html":1},languages:{pl:1},
                missing_language:1,missing_mimetype:0,
                date_min:"2026-01-01T01:02:03Z",date_max:"2026-01-02T03:04:05Z"}
             ]}}' > "$publication_report"
    fi

    if [[ "$json" == true ]]; then
        jq -cn '{schema:"warc2es.processing/v1",status:"ok",exit_code:0,
          records_in:2,records_out:2,records_indexed:null,records_skipped:0,
          errors:0,elapsed_ms:2,error:null,
          metrics:{schema:"warc2es.metrics/v1",counters:{}}}'
    else
        printf 'java-human-output\n'
    fi
}
FAKE_PIPELINE
    chmod +x "$runtime/warc2wet.sh"
}

write_warc() {
    printf 'WARC/1.0\r\nWARC-Date: 2026-01-02T03:04:05Z\r\n\r\n' > "$1"
}

run_wrapper() {
    local runtime="$1" output="$2"
    shift 2
    : > "$runtime/calls"
    set +e
    CALL_LOG="$runtime/calls" ARG_LOG="$runtime/args.nul" \
        "$runtime/warc2wet.sh" "$@" > "$output" 2> "$output.stderr"
    COMMAND_RC=$?
    set -e
}

wait_for_file() {
    local file="$1" attempt
    for ((attempt = 0; attempt < 250; attempt++)); do
        [[ -e "$file" ]] && return 0
        sleep 0.02
    done
    return 1
}

case_required_inputs_and_identifiers() {
    local runtime="$TEST_OUTPUT_DIR/required"
    local input="$runtime/input.warc"
    local empty="$runtime/empty"
    local output="$runtime/output"
    install_runtime "$runtime"
    mkdir -p "$empty"
    write_warc "$input"

    run_wrapper "$runtime" "$output" "$input"
    assert_command_failure "$COMMAND_RC" "missing --url-id succeeded" || return 1
    grep -Fq -- '--url-id is required' "$output.stderr" || return 1

    run_wrapper "$runtime" "$output" --url-id=u "$input"
    assert_command_failure "$COMMAND_RC" "missing --crawl-id succeeded" || return 1
    grep -Fq -- '--crawl-id is required' "$output.stderr" || return 1

    run_wrapper "$runtime" "$output" --url-id=u --crawl-id=c
    assert_command_failure "$COMMAND_RC" "missing positional input succeeded" || return 1
    grep -Fq 'at least one WARC input is required' "$output.stderr" || return 1

    run_wrapper "$runtime" "$output" --url-id=u --crawl-id=c "$empty"
    assert_command_failure "$COMMAND_RC" "empty WARC selection succeeded" || return 1
    grep -Fq 'no WARC input files found' "$output.stderr" || return 1
    [[ ! -s "$runtime/calls" ]] || {
        log_fail "invalid input selection invoked Java"
        return 1
    }
}

case_ordered_single_json_and_source_preservation() {
    local runtime="$TEST_OUTPUT_DIR/single"
    local root_one="$runtime/root-one"
    local root_two="$runtime/root-two"
    local output="$runtime/result.json"
    local arg
    local -a args=()
    local -a warcs=()
    install_runtime "$runtime"
    mkdir -p "$root_one/nested" "$root_two"
    write_warc "$root_one/b.warc"
    write_warc "$root_one/nested/a.warc"
    write_warc "$root_two/a.warc"

    run_wrapper "$runtime" "$output" --url-id=Site --crawl-id=Crawl \
        "$root_one" "$root_two" "$root_one/nested"
    assert_command_success "$COMMAND_RC" "single JSON extraction failed" || return 1
    [[ "$(wc -l < "$runtime/calls")" -eq 1 ]] || {
        log_fail "one extraction transaction launched more than one Java process"
        return 1
    }
    [[ "$(wc -l < "$output")" -eq 1 ]] || {
        log_fail "warc2wet JSON emitted more than one line"
        return 1
    }
    jq -e '
      .schema == "warc2es.operator/v1" and .kind == "invocation" and
      .command == "warc2wet" and .mode == "extract" and
      .status == "ok" and .exit_code == 0 and .publication == null and
      .inputs == [
        {input_index:0,path:"b.warc"},
        {input_index:0,path:"nested/a.warc"},
        {input_index:1,path:"a.warc"}
      ] and
      .outputs == ["wet/Site/Crawl/b-3files.wet.gz"] and
      .output_stats == {
        count:2,compressed_bytes:0,content_bytes:12,
        mime_types:{"application/pdf":1,"text/html":1},languages:{pl:1},
        missing_language:1,missing_mimetype:0,
        date_min:"2026-01-01T01:02:03Z",date_max:"2026-01-02T03:04:05Z",
        artifacts:[{
          path:"wet/Site/Crawl/b-3files.wet.gz",count:2,compressed_bytes:0,content_bytes:12,
          mime_types:{"application/pdf":1,"text/html":1},languages:{pl:1},
          missing_language:1,missing_mimetype:0,
          date_min:"2026-01-01T01:02:03Z",date_max:"2026-01-02T03:04:05Z"
        }]
      } and
      .processing.schema == "warc2es.processing/v1" and .error == null
    ' "$output" >/dev/null || return 1

    mapfile -d '' -t args < "$runtime/args.nul"
    for arg in "${args[@]}"; do
        [[ "$arg" == *.warc || "$arg" == *.warc.gz ]] && warcs+=("$arg")
    done
    [[ ${#warcs[@]} -eq 3 && "${warcs[0]}" == "$root_one/b.warc" &&
       "${warcs[1]}" == "$root_one/nested/a.warc" &&
       "${warcs[2]}" == "$root_two/a.warc" ]] || {
        log_fail "Java input order did not match the operator input order"
        return 1
    }
    [[ -f "$root_one/b.warc" && -f "$root_one/nested/a.warc" && -f "$root_two/a.warc" ]] || {
        log_fail "source WARC input was not preserved"
        return 1
    }
}

case_per_day_is_one_process_and_outputs_are_sorted() {
    local runtime="$TEST_OUTPUT_DIR/per-day"
    local input="$runtime/input.warc"
    local output="$runtime/result.json"
    install_runtime "$runtime"
    write_warc "$input"

    run_wrapper "$runtime" "$output" --url-id=u --crawl-id=c --per-day "$input"
    assert_command_success "$COMMAND_RC" "per-day JSON extraction failed" || return 1
    [[ "$(wc -l < "$runtime/calls")" -eq 1 ]] || {
        log_fail "per-day extraction launched more than one Java process"
        return 1
    }
    jq -e '.outputs == ["wet/u/c/input-20260101.wet.gz","wet/u/c/input-20260102.wet.gz"]' \
        "$output" >/dev/null || return 1
    jq -e '
      .output_stats.count == 2 and .output_stats.compressed_bytes == 0 and
      [.output_stats.artifacts[] | {path,count,content_bytes}] == [
        {path:"wet/u/c/input-20260101.wet.gz",count:1,content_bytes:4},
        {path:"wet/u/c/input-20260102.wet.gz",count:1,content_bytes:8}
      ]
    ' "$output" >/dev/null || return 1
    tr '\0' '\n' < "$runtime/args.nul" | grep -Fxq -- \
        '--consumer.codec.output-format=multi-warc' || return 1
    tr '\0' '\n' < "$runtime/args.nul" | grep -Fxq -- \
        '--consumer.codec.output-name-template=input-{source}.wet.gz' || return 1
}

case_processing_failure_is_wrapped() {
    local runtime="$TEST_OUTPUT_DIR/failure"
    local input="$runtime/input.warc"
    local output="$runtime/result.json"
    install_runtime "$runtime"
    write_warc "$input"

    FAKE_MODE=error run_wrapper "$runtime" "$output" --url-id=u --crawl-id=c "$input"
    [[ "$COMMAND_RC" -eq 7 ]] || {
        log_fail "Java failure exit was not preserved: $COMMAND_RC"
        return 1
    }
    jq -e '.status == "error" and .exit_code == 7 and .outputs == [] and
           .publication == null and .processing.status == "error" and
           .error.code == "processing_failed"' "$output" >/dev/null || return 1
    [[ -f "$input" ]] || {
        log_fail "failed extraction removed its source WARC"
        return 1
    }
}

case_human_mode_keeps_java_output() {
    local runtime="$TEST_OUTPUT_DIR/human"
    local input="$runtime/input.warc"
    local output="$runtime/output"
    install_runtime "$runtime"
    write_warc "$input"

    run_wrapper "$runtime" "$output" --url-id=u --crawl-id=c \
        --result-format=human "$input"
    assert_command_success "$COMMAND_RC" "human extraction failed" || return 1
    grep -Fq '[warc2wet] single' "$output" || return 1
    grep -Fq 'java-human-output' "$output" || return 1
}

case_same_pair_lock_is_busy() {
    local runtime="$TEST_OUTPUT_DIR/busy"
    local input="$runtime/input.warc"
    local output="$runtime/output"
    local lock_dir="$runtime/var/locks/warc2es/pairs/u"
    local ready="$runtime/ready" release="$runtime/release" holder
    install_runtime "$runtime"
    write_warc "$input"
    mkdir -p "$lock_dir"
    (
        exec 9>> "$lock_dir/c.lock"
        flock -x 9
        : > "$ready"
        while [[ ! -e "$release" ]]; do sleep 0.02; done
    ) &
    holder=$!
    wait_for_file "$ready" || return 1

    run_wrapper "$runtime" "$output" --url-id=u --crawl-id=c "$input"
    : > "$release"
    wait "$holder"
    [[ "$COMMAND_RC" -eq 75 ]] || {
        log_fail "same-pair extraction contention did not return 75"
        return 1
    }
    [[ ! -s "$runtime/calls" && -f "$input" ]] || {
        log_fail "busy extraction invoked Java or changed its source"
        return 1
    }
}

setup_test_env
run_stage "identifiers and a nonempty explicit selection are required" case_required_inputs_and_identifiers || true
run_stage "single JSON preserves root order, one JVM, and source WARCs" case_ordered_single_json_and_source_preservation || true
run_stage "per-day uses one JVM and reports sorted staging outputs" case_per_day_is_one_process_and_outputs_are_sorted || true
run_stage "Java processing failures are wrapped without output claims" case_processing_failure_is_wrapped || true
run_stage "human mode retains human and Java output" case_human_mode_keeps_java_output || true
run_stage "same-pair extraction contention returns busy before Java" case_same_pair_lock_is_busy || true
finish_stages
rc=$?
cleanup_test_env
exit "$rc"
