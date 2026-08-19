#!/bin/bash
# D3-001: packaged WARC -> per-day WET -> Elasticsearch provenance round trip.
# @timeout: 180
set -euo pipefail
export LC_ALL=C
# shellcheck source=/dev/null
source "$(dirname "$0")/../../lib/test-lib.sh"

ES_URL="${ES_URL:-http://localhost:9200}"
ES_USER="${ES_USER:-elastic}"
PROVENANCE_STREAM=""
PROVENANCE_STREAM_CREATED=false
ES_HTTP_CODE=""

scrub_capture_secrets() {
    local capture_root="${1:-}"
    [[ -n "$capture_root" && -d "$capture_root" ]] || return 0
    find "$capture_root" -type f -path '*/env/ES_PASS.value' -delete 2>/dev/null || true
}

cleanup_live_stream() {
    local auth=()
    [[ -z "${ES_PASS:-}" ]] || auth=(-u "$ES_USER:$ES_PASS")
    if [[ "$PROVENANCE_STREAM_CREATED" == true && -n "$PROVENANCE_STREAM" ]]; then
        curl -sS --max-time 10 -o /dev/null -X DELETE "${auth[@]}" \
            "${ES_URL%/}/_data_stream/$PROVENANCE_STREAM" 2>/dev/null || true
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

live_es_preflight() {
    local health="$TEST_OUTPUT_DIR/es-health.json"
    local template="$TEST_OUTPUT_DIR/es-template.json"

    if [[ "${RUN_DESTRUCTIVE_ES_TESTS:-false}" != true ]]; then
        if [[ "${REQUIRE_ES:-false}" == true ]]; then
            log_fail "REQUIRE_ES=true also requires RUN_DESTRUCTIVE_ES_TESTS=true"
            return 1
        fi
        log_warn "Live D3-001 round trip skipped; set RUN_DESTRUCTIVE_ES_TESTS=true"
        return 2
    fi

    if ! es_request GET '/_cluster/health' "$health" ||
       [[ "$ES_HTTP_CODE" != 200 ]] ||
       ! jq -e '.status != "red"' "$health" >/dev/null 2>&1; then
        if [[ "${REQUIRE_ES:-false}" == true ]]; then
            log_fail "Elasticsearch is required for D3-001 but is unavailable at $ES_URL"
            return 1
        fi
        log_warn "Live D3-001 round trip skipped; Elasticsearch is unavailable at $ES_URL"
        return 2
    fi

    if ! es_request GET '/_index_template/nac-data-template' "$template" ||
       [[ "$ES_HTTP_CODE" != 200 ]]; then
        log_fail "D3-001 requires nac-data-template; refusing to mutate global templates"
        return 1
    fi
}

create_live_stream() {
    local response="$TEST_OUTPUT_DIR/es-create-stream.json"
    local suffix
    IFS= read -r suffix < /proc/sys/kernel/random/uuid
    suffix="${suffix//-/}"
    PROVENANCE_STREAM="nac-data-d3-provenance-$suffix"

    if ! es_request PUT "/_data_stream/$PROVENANCE_STREAM" "$response" ||
       [[ ! "$ES_HTTP_CODE" =~ ^20[01]$ ]] ||
       ! jq -e '.acknowledged == true' "$response" >/dev/null 2>&1; then
        log_fail "Cannot create isolated D3-001 data stream $PROVENANCE_STREAM (HTTP $ES_HTTP_CODE)"
        return 1
    fi
    PROVENANCE_STREAM_CREATED=true
}

install_runtime() {
    local runtime="$1" capture_root="$2"
    local deployed="$PROJECT_ROOT/out"

    [[ -x "$deployed/warc2wet.sh" && -x "$deployed/es-upsert.sh" &&
       -x "$deployed/app/bin/es-cli" && -f "$deployed/app/lib/pipeline.jar" ]] || {
        log_fail "Packaged runtime is missing or stale; run make before D3-001"
        return 1
    }

    mkdir -p "$runtime/app/bin" "$runtime/app/var/db" "$runtime/app/log" "$capture_root"
    ln -s "$deployed/app/lib" "$runtime/app/lib"
    ln -s "$deployed/app/conf" "$runtime/app/conf"
    ln -s "$deployed/app/native" "$runtime/app/native"
    cp "$deployed/warc2wet.sh" "$deployed/es-upsert.sh" "$runtime/"
    cp "$deployed/app/bin/es-cli" "$runtime/app/bin/es-cli.real"
    chmod +x "$runtime/warc2wet.sh" "$runtime/es-upsert.sh" \
        "$runtime/app/bin/es-cli.real"
    make_call_capture_wrapper "$runtime/app/bin/es-cli" \
        "$runtime/app/bin/es-cli.real" "$capture_root"
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

extract_per_day() {
    local runtime="$1" fixture="$2" url_id="$3" crawl_id="$4" result="$5"
    local rc

    set +e
    "$runtime/warc2wet.sh" --per-day --url-id="$url_id" --crawl-id="$crawl_id" \
        --result-format=json "$fixture" >"$result" 2>"$result.stderr"
    rc=$?
    set -e
    assert_command_success "$rc" "packaged per-day extraction failed" || return 1
    assert_one_json_object "$result" "warc2wet --per-day" || return 1
    jq -e --arg url_id "$url_id" --arg crawl_id "$crawl_id" '
      .schema == "warc2es.operator/v1" and .command == "warc2wet" and
      .status == "ok" and .exit_code == 0 and
      .outputs == [
        "wet/" + $url_id + "/" + $crawl_id + "/20260101.wet.gz",
        "wet/" + $url_id + "/" + $crawl_id + "/20260102.wet.gz"
      ] and .processing.schema == "warc2es.processing/v1" and
      .processing.status == "ok"
    ' "$result" >/dev/null || {
        log_fail "per-day extraction result does not describe the two ordered WET outputs"
        return 1
    }
}

ingest_json() {
    local runtime="$1" input="$2" url_id="$3" crawl_id="$4" result="$5"
    local mode="${6:-normal}" rc endpoint="$ES_URL"
    local -a input_args=("$input" --url-id="$url_id" --crawl-id="$crawl_id")
    [[ "$mode" != archive ]] || input_args=(--from-archive "$input")

    set +e
    ES_URL="$endpoint" ES_USER="$ES_USER" ES_PASS="${ES_PASS:-}" \
        "$runtime/es-upsert.sh" "${input_args[@]}" --stream="$PROVENANCE_STREAM" \
        --es-url="$endpoint" --result-format=json >"$result" 2>"$result.stderr"
    rc=$?
    set -e
    scrub_capture_secrets "$TEST_OUTPUT_DIR/captures"
    printf '%s' "$rc"
}

count_load_calls() {
    local capture_root="$1" invocation count=0
    local -a argv=()
    for invocation in "$capture_root"/invocation-*; do
        [[ -d "$invocation" ]] || continue
        argv=()
        mapfile -d '' -t argv < "$invocation/argv.nul"
        [[ "${argv[0]:-}" != load-stream ]] || count=$((count + 1))
    done
    printf '%s\n' "$count"
}

refresh_and_snapshot() {
    local output="$1" response="$TEST_OUTPUT_DIR/es-refresh.json"
    local query
    query="$(jq -cn --arg url_id "$2" --arg crawl_id "$3" '
      {size:10, query:{bool:{filter:[
        {term:{"nac-url-id":$url_id}}, {term:{"nac-crawl-id":$crawl_id}}
      ]}}}
    ')"
    es_request POST "/$PROVENANCE_STREAM/_refresh" "$response" || return 1
    [[ "$ES_HTTP_CODE" == 200 ]] || return 1
    es_request GET "/$PROVENANCE_STREAM/_search" "$response" --data-binary "$query" || return 1
    [[ "$ES_HTTP_CODE" == 200 ]] || return 1
    jq -S '[.hits.hits[] | {_id, _source:(._source | del(."@timestamp"))}] | sort_by(._id)' \
        "$response" > "$output"
}

assert_expected_documents() {
    local snapshot="$1" url_id="$2" crawl_id="$3"
    local separator=$'\036'
    jq -e --arg url_id "$url_id" --arg crawl_id "$crawl_id" \
        --arg first_id "https://example.test/day-one${separator}2026-01-01T23:59:59Z" \
        --arg second_id "https://example.test/day-two${separator}2026-01-02T00:00:01Z" '
      length == 2 and
      .[0]._id == $first_id and
      .[0]._source."nac-url-id" == $url_id and
      .[0]._source."nac-crawl-id" == $crawl_id and
      .[0]._source."warc-uri" == "https://example.test/day-one" and
      .[0]._source."warc-date" == "2026-01-01T23:59:59Z" and
      .[0]._source."nac-first-seen" == "2026-01-01T23:59:59Z" and
      .[1]._id == $second_id and
      .[1]._source."nac-url-id" == $url_id and
      .[1]._source."nac-crawl-id" == $crawl_id and
      .[1]._source."warc-uri" == "https://example.test/day-two" and
      .[1]._source."warc-date" == "2026-01-02T00:00:01Z" and
      .[1]._source."nac-first-seen" == "2026-01-02T00:00:01Z"
    ' "$snapshot" >/dev/null || {
        log_fail "Elasticsearch documents do not match the exact D3-001 provenance/identity contract"
        return 1
    }
}

assert_mixed_provenance_preflight() {
    local deployed="$PROJECT_ROOT/out"
    local source_pair="$1" expected_url="$2" expected_crawl="$3"
    local runtime="$TEST_OUTPUT_DIR/mismatch-runtime"
    local mixed="$TEST_OUTPUT_DIR/mixed-provenance"
    local calls="$TEST_OUTPUT_DIR/mismatch-es-calls.log"
    local result="$TEST_OUTPUT_DIR/mismatch-result.json"
    local first file rc

    mkdir -p "$runtime/app/bin" "$runtime/app/var/db" "$runtime/app/log" "$mixed"
    ln -s "$deployed/app/lib" "$runtime/app/lib"
    ln -s "$deployed/app/conf" "$runtime/app/conf"
    ln -s "$deployed/app/native" "$runtime/app/native"
    cp "$deployed/es-upsert.sh" "$runtime/es-upsert.sh"
    chmod +x "$runtime/es-upsert.sh"
    make_fake_es_cli "$runtime/app/bin/es-cli" "$calls"

    while IFS= read -r -d '' file; do
        cp "$file" "$mixed/$(basename "$file")"
    done < <(find "$source_pair" -maxdepth 1 -type f -name '*.wet.gz' -print0 | sort -z)
    first="$(find "$mixed" -maxdepth 1 -type f -name '*.wet.gz' -print | sort | head -n 1)"
    [[ -n "$first" ]] || return 1
    gzip -cd -- "$first" | sed "s/X-NAC-Crawl-ID: $expected_crawl/X-NAC-Crawl-ID: intruder/g" \
        | gzip > "$first.changed"
    mv "$first.changed" "$first"

    set +e
    "$runtime/es-upsert.sh" "$mixed" --stream="$PROVENANCE_STREAM" \
        --url-id="$expected_url" --crawl-id="$expected_crawl" --es-url="$ES_URL" \
        --result-format=json >"$result" 2>"$result.stderr"
    rc=$?
    set -e
    assert_command_failure "$rc" "mixed provenance unexpectedly reached Elasticsearch" || return 1
    [[ ! -s "$calls" ]] || {
        log_fail "mixed-provenance validation invoked es-cli before rejecting the set"
        return 1
    }
}

test_provenance_roundtrip() {
    local runtime="$TEST_OUTPUT_DIR/runtime"
    local capture_root="$TEST_OUTPUT_DIR/captures"
    local fixture="$PROJECT_ROOT/src/test/resources/multi-day.warc.gz"
    local url_a="d3-provenance-a" crawl_a="crawl-a"
    local url_b="d3-provenance-b" crawl_b="crawl-b"
    local pair_a pair_b result rc preflight_rc=0
    local before="$TEST_OUTPUT_DIR/before-retry.json"
    local after="$TEST_OUTPUT_DIR/after-retry.json"
    local pair_b_docs="$TEST_OUTPUT_DIR/pair-b.json"

    assert_file_exists "$fixture" || return 1
    live_es_preflight || preflight_rc=$?
    case "$preflight_rc" in
        0) ;;
        2)
            echo "TESTCASE|provenance-roundtrip|SKIP|reason=explicit-live-precondition"
            return 0
            ;;
        *) return 1 ;;
    esac

    install_runtime "$runtime" "$capture_root" || return 1
    create_live_stream || return 1

    result="$TEST_OUTPUT_DIR/extract-a.json"
    extract_per_day "$runtime" "$fixture" "$url_a" "$crawl_a" "$result" || return 1
    pair_a="$runtime/wet/$url_a/$crawl_a"
    [[ "$(find "$pair_a" -maxdepth 1 -type f -name '*.wet.gz' | wc -l)" -eq 2 ]] || return 1

    result="$TEST_OUTPUT_DIR/ingest-a.json"
    rc="$(ingest_json "$runtime" "$pair_a" "$url_a" "$crawl_a" "$result")"
    assert_command_success "$rc" "two-WET provenance transaction failed" || return 1
    assert_one_json_object "$result" "initial es-upsert" || return 1
    jq -e '.status == "ok" and .processing.status == "ok" and
           .publication.status == "published" and (.publication.paths | length) == 2' \
        "$result" >/dev/null || return 1
    [[ "$(count_load_calls "$capture_root")" -eq 1 ]] || {
        log_fail "initial two-WET transaction did not use exactly one Java load process"
        return 1
    }

    pair_a="$runtime/all/wet/$url_a/$crawl_a"
    [[ "$(find "$pair_a" -maxdepth 1 -type f -name '*.wet.gz' | wc -l)" -eq 2 ]] || return 1
    refresh_and_snapshot "$before" "$url_a" "$crawl_a" || return 1
    assert_expected_documents "$before" "$url_a" "$crawl_a" || return 1

    result="$TEST_OUTPUT_DIR/retry-a.json"
    rc="$(ingest_json "$runtime" "$pair_a" "$url_a" "$crawl_a" "$result" archive)"
    assert_command_success "$rc" "published-WET retry was not idempotent" || return 1
    assert_one_json_object "$result" "published-WET retry" || return 1
    jq -e '.status == "ok" and .processing.status == "ok" and
           .publication.status == "unchanged" and (.publication.paths | length) == 2' \
        "$result" >/dev/null || return 1
    [[ "$(count_load_calls "$capture_root")" -eq 2 ]] || {
        log_fail "published-WET retry returned success without its Java load process"
        return 1
    }
    refresh_and_snapshot "$after" "$url_a" "$crawl_a" || return 1
    cmp -s "$before" "$after" || {
        log_fail "identical transaction retry changed stable Elasticsearch documents"
        return 1
    }

    result="$TEST_OUTPUT_DIR/extract-b.json"
    extract_per_day "$runtime" "$fixture" "$url_b" "$crawl_b" "$result" || return 1
    pair_b="$runtime/wet/$url_b/$crawl_b"
    result="$TEST_OUTPUT_DIR/ingest-b.json"
    rc="$(ingest_json "$runtime" "$pair_b" "$url_b" "$crawl_b" "$result")"
    assert_command_failure "$rc" "cross-provenance document collision was accepted" || return 1
    assert_one_json_object "$result" "conflicting es-upsert" || return 1
    jq -e '.status == "error" and .exit_code != 0 and .processing.status == "error" and
           .processing.error.code == "after_check_failed" and
           .processing.metrics.counters.es_exporter_vt.bulk_errors == 2 and
           .publication.status == "skipped"' "$result" >/dev/null || return 1
    [[ "$(count_load_calls "$capture_root")" -eq 3 ]] || {
        log_fail "collision result did not come from the expected Java load process"
        return 1
    }
    [[ ! -e "$runtime/all/wet/$url_b/$crawl_b" ]] || {
        [[ -z "$(find "$runtime/all/wet/$url_b/$crawl_b" -maxdepth 1 -type f -name '*.wet.gz' -print -quit)" ]] || {
            log_fail "failed collision transaction published pair B"
            return 1
        }
    }
    refresh_and_snapshot "$after" "$url_a" "$crawl_a" || return 1
    cmp -s "$before" "$after" || {
        log_fail "failed pair-B collision changed pair-A documents"
        return 1
    }
    refresh_and_snapshot "$pair_b_docs" "$url_b" "$crawl_b" || return 1
    jq -e 'length == 0' "$pair_b_docs" >/dev/null || return 1

    assert_mixed_provenance_preflight "$pair_b" "$url_b" "$crawl_b" || return 1
}

run_test test_provenance_roundtrip
