#!/bin/bash
# tc-es-005-ingest-idempotency.sh
# T-088: Repeating a direct create load must fail and preserve the stream count.
# @timeout: 120
set -euo pipefail
# shellcheck source=/dev/null
source "$(dirname "$0")/../../../../lib/test-lib.sh"

ES_URL="${ES_URL:-http://localhost:9200}"
ES_USER="${ES_USER:-elastic}"
ES_PASS="${ES_PASS:-${ELASTIC_PASSWORD:-}}"
export ES_URL ES_USER ES_PASS
ES_AUTH=()
[[ -z "$ES_PASS" ]] || ES_AUTH=(-u "$ES_USER:$ES_PASS")
TEST_STREAM=""
TEST_STREAM_CREATED=false

cleanup_stream() {
    [[ "$TEST_STREAM_CREATED" != true ]] || "$ES_CLI" delete-stream "$TEST_STREAM" &>/dev/null || true
}
trap cleanup_stream EXIT

make_wet() {
    local output="$1" url_id="$2" crawl_id="$3" uri="$4"
    local date="2026-08-02T00:00:00Z"
    local content="idempotency collision fixture"
    local length=${#content}

    printf 'WARC/1.0\r\nWARC-Type: conversion\r\nWARC-Target-URI: %s\r\nWARC-Date: %s\r\nWARC-Record-ID: <urn:uuid:%s>\r\nWARC-Block-Digest: sha256:%s\r\nX-NAC-First-Seen: %s\r\nX-NAC-URL-ID: %s\r\nX-NAC-Crawl-ID: %s\r\nContent-Type: text/plain\r\nContent-Length: %s\r\n\r\n%s\r\n\r\n' \
        "$uri" "$date" "$(< /proc/sys/kernel/random/uuid)" \
        'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' \
        "$date" "$url_id" "$crawl_id" "$length" "$content" | gzip > "$output"
}

stream_count() {
    local stream="$1"

    curl -fsS "${ES_AUTH[@]}" -H 'Content-Type: application/json' \
        -d '{"query":{"match_all":{}}}' "$ES_URL/$stream/_count" |
        python3 -c 'import json, sys; print(json.load(sys.stdin)["count"])'
}

require_data_template() {
    curl -fsS "${ES_AUTH[@]}" "$ES_URL/_index_template/nac-data-template" |
        python3 -c 'import json, sys; sys.exit(0 if json.load(sys.stdin).get("index_templates") else 1)'
}

create_test_stream() {
    local response status body

    if ! response=$(curl -fsS -X PUT "${ES_AUTH[@]}" -H 'Content-Type: application/json' \
        -w $'\n%{http_code}' "$ES_URL/_data_stream/$TEST_STREAM"); then
        log_fail "Could not create test stream $TEST_STREAM"
        return 1
    fi
    status="${response##*$'\n'}"
    body="${response%$'\n'*}"
    if [[ "$status" != 200 && "$status" != 201 ]] ||
        ! printf '%s' "$body" | python3 -c 'import json, sys; sys.exit(0 if json.load(sys.stdin).get("acknowledged") is True else 1)'; then
        log_fail "Creating test stream $TEST_STREAM was not acknowledged (HTTP $status)"
        return 1
    fi
    TEST_STREAM_CREATED=true
}

test_ingest_idempotency() {
    if [[ "${RUN_DESTRUCTIVE_ES_TESTS:-false}" != true ]]; then
        if [[ "${REQUIRE_ES:-false}" == true ]]; then
            log_fail "REQUIRE_ES=true also requires RUN_DESTRUCTIVE_ES_TESTS=true"
            return 1
        fi
        log_warn "Live tc-es-005 skipped; set RUN_DESTRUCTIVE_ES_TESTS=true"
        echo "TESTCASE|ingest-idempotency|SKIP|reason=explicit-live-precondition"
        return 0
    fi
    if ! "$ES_CLI" check-health &>/dev/null; then
        if [[ "${REQUIRE_ES:-false}" == true ]]; then
            log_fail "Elasticsearch is required for tc-es-005 but unavailable at $ES_URL"
            return 1
        fi
        log_warn "Elasticsearch not available — skipping"
        echo "TESTCASE|ingest-idempotency|SKIP|reason=elasticsearch-unavailable"
        return 0
    fi

    local suffix
    suffix=$(< /proc/sys/kernel/random/uuid)
    local wet="$TEST_OUTPUT_DIR/tc-es-005-$suffix.wet.gz"
    local url_id="tc-es-005-url-$suffix"
    local crawl_id="tc-es-005-crawl-$suffix"
    TEST_STREAM="nac-data-idempotency-$suffix"

    make_wet "$wet" "$url_id" "$crawl_id" "https://example.invalid/idempotency/$suffix" || return 1
    if ! require_data_template; then
        log_fail "Required nac-data-template is missing or inaccessible"
        return 1
    fi
    create_test_stream || return 1

    log_info "First direct load into $TEST_STREAM..."
    "$ES_CLI" load-stream "$wet" "$TEST_STREAM" \
        --url-id="$url_id" --crawl-id="$crawl_id" || return 1
    "$ES_CLI" refresh "$TEST_STREAM" &>/dev/null || return 1
    local count1
    count1=$(stream_count "$TEST_STREAM") || return 1
    if [[ "$count1" -ne 1 ]]; then
        log_fail "First direct load indexed $count1 documents, expected 1"
        echo "TESTCASE|ingest-idempotency|FAIL|count1=$count1"
        return 1
    fi

    log_info "Second direct load (same record, same provenance)..."
    if "$ES_CLI" load-stream "$wet" "$TEST_STREAM" --url-id="$url_id" --crawl-id="$crawl_id"; then
        log_fail "Second direct load unexpectedly succeeded despite create conflict"
        echo "TESTCASE|ingest-idempotency|FAIL|second-load-succeeded,count1=$count1"
        return 1
    fi

    "$ES_CLI" refresh "$TEST_STREAM" &>/dev/null || return 1
    local count2
    count2=$(stream_count "$TEST_STREAM") || return 1
    if [[ "$count1" -ne "$count2" ]]; then
        log_fail "Count changed after rejected second load: $count1 -> $count2"
        echo "TESTCASE|ingest-idempotency|FAIL|count1=$count1,count2=$count2"
        return 1
    fi

    log_info "Rejected duplicate direct load preserved count: $count1"
    echo "TESTCASE|ingest-idempotency|PASS|count=$count1,second-load=rejected"
}

run_test test_ingest_idempotency
