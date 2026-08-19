#!/bin/bash
# D3-005: real Java processing results survive the shell operator boundary.
set -euo pipefail
export LC_ALL=C
source "$(dirname "$0")/../../lib/test-lib.sh"

ES_URL="${ES_URL:-http://localhost:9200}"
ES_USER="${ES_USER:-elastic}"
XLAYER_STREAM=""
XLAYER_STREAM_CREATED=false
ES_HTTP_CODE=""

scrub_capture_secrets() {
    local capture_root="${1:-}"
    [[ -n "$capture_root" && -d "$capture_root" ]] || return 0
    find "$capture_root" -type f -path '*/env/ES_PASS.value' -delete 2>/dev/null || true
}

cleanup_live_stream() {
    local auth=()
    [[ -z "${ES_PASS:-}" ]] || auth=(-u "$ES_USER:$ES_PASS")
    if [[ "$XLAYER_STREAM_CREATED" == true && -n "$XLAYER_STREAM" ]]; then
        curl -sS --max-time 10 -o /dev/null -X DELETE "${auth[@]}" \
            "${ES_URL%/}/_data_stream/$XLAYER_STREAM" 2>/dev/null || true
    fi
    scrub_capture_secrets "${TEST_OUTPUT_DIR:-}/captures"
}
trap cleanup_live_stream EXIT

es_request() {
    local method="$1" path="$2" output="$3"
    shift 3
    local auth=()
    [[ -z "${ES_PASS:-}" ]] || auth=(-u "$ES_USER:$ES_PASS")
    ES_HTTP_CODE="$(curl -sS --connect-timeout 5 --max-time 20 \
        -o "$output" -w '%{http_code}' -X "$method" "${auth[@]}" \
        -H 'Content-Type: application/json' "${ES_URL%/}$path" "$@")" || {
        ES_HTTP_CODE=000
        return 1
    }
}

assert_one_json_object() {
    local file="$1" label="$2"
    [[ -s "$file" && "$(wc -l < "$file")" -eq 1 ]] || {
        log_fail "$label did not emit exactly one newline-terminated JSON object"
        return 1
    }
    jq -e 'type == "object"' "$file" >/dev/null 2>&1 || {
        log_fail "$label stdout is not one JSON object"
        return 1
    }
}

install_runtime() {
    local runtime="$1" capture_root="$2"
    local deployed="$PROJECT_ROOT/out"

    [[ -x "$deployed/warc2wet.sh" && -x "$deployed/es-upsert.sh" &&
       -x "$deployed/es-upsert-all.sh" && -x "$deployed/app/bin/es-cli" &&
       -f "$deployed/app/lib/pipeline.jar" ]] || {
        log_fail "Packaged runtime is missing or stale; run make before D3-005"
        return 1
    }

    mkdir -p "$runtime/app/bin" "$runtime/app/var/db" "$runtime/app/log" "$capture_root"
    ln -s "$deployed/app/lib" "$runtime/app/lib"
    ln -s "$deployed/app/conf" "$runtime/app/conf"
    ln -s "$deployed/app/native" "$runtime/app/native"
    cp "$deployed/warc2wet.sh" "$deployed/es-upsert.sh" \
        "$deployed/es-upsert-all.sh" "$runtime/"
    cp "$deployed/app/bin/es-cli" "$runtime/app/bin/es-cli.real"
    chmod +x "$runtime/warc2wet.sh" "$runtime/es-upsert.sh" \
        "$runtime/es-upsert-all.sh" "$runtime/app/bin/es-cli.real"
    make_call_capture_wrapper "$runtime/app/bin/es-cli" \
        "$runtime/app/bin/es-cli.real" "$capture_root"
}

run_extraction() {
    local runtime="$1" fixture="$2" url_id="$3" crawl_id="$4"
    local result="$5" stderr_file="$6"
    local rc source_name expected_output

    source_name="$(basename "$fixture")"
    source_name="${source_name%.warc.gz}"
    expected_output="wet/$url_id/$crawl_id/$source_name.wet.gz"

    set +e
    "$runtime/warc2wet.sh" --url-id="$url_id" --crawl-id="$crawl_id" \
        --result-format=json "$fixture" >"$result" 2>"$stderr_file"
    rc=$?
    set -e
    assert_command_success "$rc" "real packaged extraction failed" || return 1
    assert_one_json_object "$result" "warc2wet" || return 1
    [[ -s "$stderr_file" ]] || {
        log_fail "warc2wet diagnostics were not separated onto stderr"
        return 1
    }
    jq -e --arg expected_output "$expected_output" '
      .schema == "warc2es.operator/v1" and .kind == "invocation" and
      .command == "warc2wet" and .mode == "extract" and
      .status == "ok" and .exit_code == 0 and .publication == null and
      (.inputs | type == "array" and length == 1) and
      .outputs == [$expected_output] and
      (.processing | type == "object") and
      .processing.schema == "warc2es.processing/v1" and
      .processing.status == "ok" and .error == null
    ' "$result" >/dev/null || {
        log_fail "warc2wet envelope does not contain the real processing object and staged output"
        return 1
    }
}

live_es_preflight() {
    local health="$TEST_OUTPUT_DIR/es-health.json"
    local template="$TEST_OUTPUT_DIR/es-template.json"

    if [[ "${RUN_DESTRUCTIVE_ES_TESTS:-false}" != true ]]; then
        if [[ "${REQUIRE_ES:-false}" == true ]]; then
            log_fail "REQUIRE_ES=true also requires RUN_DESTRUCTIVE_ES_TESTS=true"
            return 1
        fi
        log_warn "Live D3-005 ingestion skipped; set RUN_DESTRUCTIVE_ES_TESTS=true"
        return 2
    fi

    if ! es_request GET '/_cluster/health' "$health" ||
       [[ "$ES_HTTP_CODE" != 200 ]] ||
       ! jq -e '.status != "red"' "$health" >/dev/null 2>&1; then
        if [[ "${REQUIRE_ES:-false}" == true ]]; then
            log_fail "Elasticsearch is required for D3-005 but is unavailable at $ES_URL"
            return 1
        fi
        log_warn "Live D3-005 ingestion skipped; Elasticsearch is unavailable at $ES_URL"
        return 2
    fi

    if ! es_request GET '/_index_template/nac-data-template' "$template" ||
       [[ "$ES_HTTP_CODE" != 200 ]]; then
        log_fail "D3-005 requires the installed nac-data-template; refusing to mutate global templates"
        return 1
    fi
}

create_live_stream() {
    local response="$TEST_OUTPUT_DIR/es-create-stream.json"
    local suffix
    IFS= read -r suffix < /proc/sys/kernel/random/uuid
    suffix="${suffix//-/}"
    XLAYER_STREAM="nac-data-d3-xlayer-$suffix"

    if ! es_request PUT "/_data_stream/$XLAYER_STREAM" "$response" ||
       [[ ! "$ES_HTTP_CODE" =~ ^20[01]$ ]] ||
       ! jq -e '.acknowledged == true' "$response" >/dev/null 2>&1; then
        log_fail "Cannot create isolated D3-005 data stream $XLAYER_STREAM (HTTP $ES_HTTP_CODE)"
        return 1
    fi
    XLAYER_STREAM_CREATED=true
}

find_only_load_capture() {
    local capture_root="$1" output_name="$2"
    local -n output_ref="$output_name"
    local invocation
    local -a argv=()
    local -a matches=()

    for invocation in "$capture_root"/invocation-*; do
        [[ -d "$invocation" ]] || continue
        argv=()
        mapfile -d '' -t argv < "$invocation/argv.nul"
        [[ "${argv[0]:-}" == load-stream ]] && matches+=("$invocation")
    done
    [[ ${#matches[@]} -eq 1 ]] || {
        log_fail "Expected exactly one captured Java load, found ${#matches[@]}"
        return 1
    }
    output_ref="${matches[0]}"
}

run_ingestion_and_compare() {
    local runtime="$1" url_id="$2" crawl_id="$3" capture_root="$4"
    local result="$5" stderr_file="$6"
    local load_capture="" rc

    set +e
    ES_URL="$ES_URL" ES_USER="$ES_USER" ES_PASS="${ES_PASS:-}" \
        "$runtime/es-upsert.sh" --stream="$XLAYER_STREAM" \
        --url-id="$url_id" --crawl-id="$crawl_id" --es-url="$ES_URL" \
        --result-format=json >"$result" 2>"$stderr_file"
    rc=$?
    set -e
    scrub_capture_secrets "$capture_root"
    assert_command_success "$rc" "real packaged ingestion failed" || return 1
    assert_one_json_object "$result" "es-upsert" || return 1
    [[ -s "$stderr_file" ]] || {
        log_fail "es-upsert diagnostics were not separated onto stderr"
        return 1
    }
    jq -e '
      .schema == "warc2es.operator/v1" and .kind == "invocation" and
      .command == "es-upsert" and .status == "ok" and .exit_code == 0 and
      (.processing | type == "object") and
      .processing.schema == "warc2es.processing/v1" and
      .processing.status == "ok" and
      .publication.status == "published" and
      (.publication.paths | type == "array" and length == 1) and .error == null
    ' "$result" >/dev/null || {
        log_fail "es-upsert envelope does not contain a successful Java processing object"
        return 1
    }

    find_only_load_capture "$capture_root" load_capture || return 1
    assert_one_json_object "$load_capture/stdout" "captured Java load" || return 1
    jq -e '.schema == "warc2es.processing/v1" and .status == "ok"' \
        "$load_capture/stdout" >/dev/null || return 1
    jq -S . "$load_capture/stdout" > "$TEST_OUTPUT_DIR/java-processing.normalized.json"
    jq -S .processing "$result" > "$TEST_OUTPUT_DIR/envelope-processing.normalized.json"
    cmp -s "$TEST_OUTPUT_DIR/java-processing.normalized.json" \
        "$TEST_OUTPUT_DIR/envelope-processing.normalized.json" || {
        log_fail "Shell processing member differs from the captured Java stdout object"
        return 1
    }
}

run_second_pair() {
    local runtime="$1" fixture="$2" url_id="$3" crawl_id="$4"
    local result="$TEST_OUTPUT_DIR/second-extract.json"
    local ingest_result="$TEST_OUTPUT_DIR/second-ingest.json"
    local rc

    run_extraction "$runtime" "$fixture" "$url_id" "$crawl_id" \
        "$result" "$result.stderr" || return 1
    set +e
    ES_URL="$ES_URL" ES_USER="$ES_USER" ES_PASS="${ES_PASS:-}" \
        "$runtime/es-upsert.sh" --stream="$XLAYER_STREAM" \
        --url-id="$url_id" --crawl-id="$crawl_id" \
        --es-url="$ES_URL" --result-format=json \
        >"$ingest_result" 2>"$ingest_result.stderr"
    rc=$?
    set -e
    scrub_capture_secrets "$TEST_OUTPUT_DIR/captures"
    assert_command_success "$rc" "second real ingestion failed" || return 1
    assert_one_json_object "$ingest_result" "second es-upsert" || return 1
    jq -e '.status == "ok" and .processing.schema == "warc2es.processing/v1" and
           .publication.status == "published"' "$ingest_result" >/dev/null
}

assert_ordered_replay_batch() {
    local runtime="$1" first_pair="$2" second_pair="$3"
    local output="$TEST_OUTPUT_DIR/replay.ndjson"
    local stderr_file="$TEST_OUTPUT_DIR/replay.stderr"
    local rc

    set +e
    ES_URL="$ES_URL" ES_USER="$ES_USER" ES_PASS="${ES_PASS:-}" \
        "$runtime/es-upsert-all.sh" --stream="$XLAYER_STREAM" --es-url="$ES_URL" \
        >"$output" 2>"$stderr_file"
    rc=$?
    set -e
    scrub_capture_secrets "$TEST_OUTPUT_DIR/captures"
    assert_command_success "$rc" "real archive replay batch failed" || return 1
    [[ "$(wc -l < "$output")" -eq 3 ]] || {
        log_fail "Replay batch did not emit two invocations plus one summary"
        return 1
    }
    [[ -s "$stderr_file" ]] || {
        log_fail "Replay diagnostics were not separated onto stderr"
        return 1
    }
    jq -e -s --arg first "all/wet/$first_pair/" --arg second "all/wet/$second_pair/" '
      length == 3 and
      ([.[] | select(.kind == "summary")] | length) == 1 and
      .[0].kind == "invocation" and .[0].command == "es-upsert" and
      .[0].mode == "archive-replay" and .[0].status == "ok" and
      (.[0].processing | type == "object") and
      (.[0].publication.paths | length > 0) and
      all(.[0].publication.paths[]; startswith($first)) and
      .[1].kind == "invocation" and .[1].command == "es-upsert" and
      .[1].mode == "archive-replay" and .[1].status == "ok" and
      (.[1].processing | type == "object") and
      (.[1].publication.paths | length > 0) and
      all(.[1].publication.paths[]; startswith($second)) and
      .[2].kind == "summary" and .[2].command == "es-upsert-all" and
      .[2].status == "ok" and .[2].exit_code == 0 and
      .[2].total == 2 and .[2].succeeded == 2 and .[2].failed == 0
    ' "$output" >/dev/null || {
        log_fail "Replay NDJSON order or final-summary contract failed"
        return 1
    }
}

test_cross_layer_result_envelope() {
    local runtime="$TEST_OUTPUT_DIR/runtime"
    local capture_root="$TEST_OUTPUT_DIR/captures"
    local fixture="$PROJECT_ROOT/src/test/resources/multi-day.warc.gz"
    local shifted_fixture="$TEST_OUTPUT_DIR/multi-day-shifted.warc.gz"
    local first_url="z-xlayer" first_crawl="crawl-z"
    local second_url="a-xlayer" second_crawl="crawl-a"
    local extraction_result="$TEST_OUTPUT_DIR/extraction.json"
    local ingestion_result="$TEST_OUTPUT_DIR/ingestion.json"
    local preflight_rc=0

    assert_file_exists "$fixture" || return 1
    install_runtime "$runtime" "$capture_root" || return 1
    run_extraction "$runtime" "$fixture" "$first_url" "$first_crawl" \
        "$extraction_result" "$extraction_result.stderr" || return 1

    live_es_preflight || preflight_rc=$?
    case "$preflight_rc" in
        0) ;;
        2)
            echo "TESTCASE|xlayer-live-ingestion|SKIP|reason=explicit-live-precondition"
            return 0
            ;;
        *) return 1 ;;
    esac
    create_live_stream || return 1

    run_ingestion_and_compare "$runtime" "$first_url" "$first_crawl" \
        "$capture_root" "$ingestion_result" "$ingestion_result.stderr" || return 1

    gzip -cd -- "$fixture" | \
        sed -e 's/2026-01-01/2026-02-01/g' -e 's/2026-01-02/2026-02-02/g' | \
        gzip > "$shifted_fixture"
    run_second_pair "$runtime" "$shifted_fixture" "$second_url" "$second_crawl" || return 1
    assert_ordered_replay_batch "$runtime" "$second_url/$second_crawl" \
        "$first_url/$first_crawl"
}

run_test test_cross_layer_result_envelope
