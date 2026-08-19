#!/bin/bash
# B2-002: SHA-path replay and capture-date input order produce identical documents.
set -euo pipefail
export LC_ALL=C
source "$(dirname "$0")/../../lib/test-lib.sh"

ES_URL="${ES_URL:-http://localhost:9200}"
ORDER_STREAM_SHA=""
ORDER_STREAM_DATE=""
ORDER_STREAM_ZERO=""

cleanup_streams() {
    [[ -z "$ORDER_STREAM_SHA" ]] || ES_URL="$ES_URL" "$ES_CLI" delete-stream "$ORDER_STREAM_SHA" &>/dev/null || true
    [[ -z "$ORDER_STREAM_DATE" ]] || ES_URL="$ES_URL" "$ES_CLI" delete-stream "$ORDER_STREAM_DATE" &>/dev/null || true
    [[ -z "$ORDER_STREAM_ZERO" ]] || ES_URL="$ES_URL" "$ES_CLI" delete-stream "$ORDER_STREAM_ZERO" &>/dev/null || true
}
trap cleanup_streams EXIT

make_wet() {
    local output="$1" url_id="$2" crawl_id="$3" uri="$4" date="$5" content="$6"
    local length=${#content}
    printf 'WARC/1.0\r\nWARC-Type: conversion\r\nWARC-Target-URI: %s\r\nWARC-Date: %sT00:00:00Z\r\nWARC-Record-ID: <urn:uuid:%s>\r\nWARC-Block-Digest: sha256:%s\r\nX-NAC-First-Seen: %s\r\nX-NAC-URL-ID: %s\r\nX-NAC-Crawl-ID: %s\r\nContent-Type: text/plain\r\nContent-Length: %s\r\n\r\n%s\r\n\r\n' \
        "$uri" "$date" "${date//-/}" "${date//-/}" "$date" "$url_id" "$crawl_id" \
        "$length" "$content" | gzip > "$output"
}

stream_documents() {
    local stream="$1"
    local auth=()
    [[ -z "${ES_PASS:-}" ]] || auth=(-u "${ES_USER:-elastic}:${ES_PASS}")
    curl -fsS "${auth[@]}" -H 'Content-Type: application/json' \
      "$ES_URL/$stream/_search?size=10000" \
      -d '{"query":{"match_all":{}}}' \
      | jq -S '[.hits.hits[] | {_id:._id,_source:(._source | del(."@timestamp"))}] | sort_by(._id)'
}

snapshot_pair() {
    local pair="$1"
    local output="$2"
    local file
    : > "$output"
    stat -c '%n %F %a %i %s %Y' -- "$pair" >> "$output" || return 1
    while IFS= read -r -d '' file; do
        stat -c '%n %F %a %i %s %Y' -- "$file" >> "$output" || return 1
        sha256sum -- "$file" >> "$output" || return 1
    done < <(find "$pair" -mindepth 1 -maxdepth 1 -print0 | LC_ALL=C sort -z)
}

setup_runtime() {
    local runtime="$1"
    local deployed="$PROJECT_ROOT/out"
    mkdir -p "$runtime"
    ln -s "$deployed/app" "$runtime/app"
    cp "$deployed/es-upsert.sh" "$runtime/es-upsert.sh"
    chmod +x "$runtime/es-upsert.sh"
}

test_replay_order_independence() {
    local deployed="$PROJECT_ROOT/out"
    if [[ ! -x "$deployed/app/bin/es-cli" || ! -x "$deployed/es-upsert.sh" ]]; then
        log_fail "Packaged runtime is missing; run make before this integration test"
        return 1
    fi
    if ! ES_URL="$ES_URL" "$ES_CLI" check-health &>/dev/null; then
        if [[ "${REQUIRE_ES:-false}" == true ]]; then
            log_fail "Elasticsearch is required for the B2-002 acceptance proof"
            return 1
        fi
        log_warn "Elasticsearch not available - skipping"
        return 0
    fi

    local runtime_sha="$TEST_OUTPUT_DIR/runtime-sha"
    local runtime_date="$TEST_OUTPUT_DIR/runtime-date"
    local runtime_zero="$TEST_OUTPUT_DIR/runtime-zero"
    local source_dir="$TEST_OUTPUT_DIR/source"
    local sha_input="$TEST_OUTPUT_DIR/sha-order"
    local date_input="$TEST_OUTPUT_DIR/date-order"
    local first="$source_dir/first.wet.gz"
    local second="$source_dir/second.wet.gz"
    local first_sha second_sha salt=0 archive_before archive_after published_date_pair
    local zero_input="$TEST_OUTPUT_DIR/zero.wet.gz" zero_result="$TEST_OUTPUT_DIR/zero-result.json"
    local empty_sha=e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
    local url_id="b2orderurl"
    local crawl_id="b2ordercrawl"
    local suffix
    suffix="$(date +%s%N)"
    ORDER_STREAM_SHA="nac-data-b2-sha-$suffix"
    ORDER_STREAM_DATE="nac-data-b2-date-$suffix"
    ORDER_STREAM_ZERO="nac-data-b2-zero-$suffix"

    mkdir -p "$source_dir" "$sha_input" "$date_input"
    while true; do
        make_wet "$first" "$url_id" "$crawl_id" "https://example.test/shared" \
            2026-01-01 "first-$salt"
        make_wet "$second" "$url_id" "$crawl_id" "https://example.test/shared" \
            2026-01-02 "second-$salt"
        first_sha="$(sha256sum "$first" | awk '{print $1}')"
        second_sha="$(sha256sum "$second" | awk '{print $1}')"
        [[ "$first_sha" > "$second_sha" ]] && break
        salt=$((salt + 1))
        [[ "$salt" -lt 50 ]] || { log_fail "Could not construct opposite SHA/date order"; return 1; }
    done

    cp "$first" "$sha_input/$first_sha.wet.gz"
    cp "$second" "$sha_input/$second_sha.wet.gz"
    cp "$first" "$date_input/20260101.wet.gz"
    cp "$second" "$date_input/20260102.wet.gz"
    setup_runtime "$runtime_sha"
    setup_runtime "$runtime_date"
    setup_runtime "$runtime_zero"
    mkdir -p "$runtime_sha/all/wet/$url_id/$crawl_id"
    cp "$sha_input"/*.wet.gz "$runtime_sha/all/wet/$url_id/$crawl_id/"
    archive_before="$TEST_OUTPUT_DIR/archive-before"
    archive_after="$TEST_OUTPUT_DIR/archive-after"
    snapshot_pair "$runtime_sha/all/wet/$url_id/$crawl_id" "$archive_before" || return 1

    "$deployed/es-reinit.sh" --stream="$ORDER_STREAM_SHA" --es-url="$ES_URL" --yes >/dev/null || return 1
    "$deployed/es-reinit.sh" --stream="$ORDER_STREAM_DATE" --es-url="$ES_URL" --yes >/dev/null || return 1
    "$deployed/es-reinit.sh" --stream="$ORDER_STREAM_ZERO" --es-url="$ES_URL" --yes >/dev/null || return 1

    "$runtime_sha/es-upsert.sh" --from-archive "$runtime_sha/all/wet/$url_id/$crawl_id" \
      --stream="$ORDER_STREAM_SHA" --start-date=2026-01-01 --es-url="$ES_URL" || return 1
    "$runtime_date/es-upsert.sh" "$date_input" --url-id="$url_id" --crawl-id="$crawl_id" \
      --stream="$ORDER_STREAM_DATE" --start-date=2026-01-01 --es-url="$ES_URL" || return 1

    published_date_pair="$runtime_date/all/wet/$url_id/$crawl_id"
    [[ -f "$published_date_pair/$first_sha.wet.gz" &&
       -f "$published_date_pair/$second_sha.wet.gz" &&
       "$(find "$published_date_pair" -mindepth 1 -maxdepth 1 -type f -name '*.wet.gz' | wc -l)" -eq 2 ]] || {
        log_fail "Normal multi-file ingestion did not publish the exact two-SHA set"
        return 1
    }

    : > "$zero_input"
    "$runtime_zero/es-upsert.sh" "$zero_input" --url-id=b2zerourl --crawl-id=b2zerocrawl \
      --stream="$ORDER_STREAM_ZERO" --es-url="$ES_URL" --result-format=json \
      > "$zero_result" || return 1
    jq -e '.status == "ok" and .processing == null' "$zero_result" >/dev/null || {
        log_fail "Real zero-record transaction did not report a shell-only success"
        return 1
    }
    [[ -f "$runtime_zero/all/wet/b2zerourl/b2zerocrawl/$empty_sha.wet.gz" ]] || {
        log_fail "Real zero-record transaction did not publish the zero-byte SHA"
        return 1
    }
    ES_URL="$ES_URL" "$ES_CLI" refresh "$ORDER_STREAM_ZERO" >/dev/null || return 1
    stream_documents "$ORDER_STREAM_ZERO" | jq -e 'length == 0' >/dev/null || {
        log_fail "Real zero-record transaction created Elasticsearch documents"
        return 1
    }

    snapshot_pair "$runtime_sha/all/wet/$url_id/$crawl_id" "$archive_after" || return 1
    cmp -s "$archive_before" "$archive_after" || {
        log_fail "Archive replay mutated the published provenance set"
        return 1
    }
    ES_URL="$ES_URL" "$ES_CLI" refresh "$ORDER_STREAM_SHA" >/dev/null || return 1
    ES_URL="$ES_URL" "$ES_CLI" refresh "$ORDER_STREAM_DATE" >/dev/null || return 1
    stream_documents "$ORDER_STREAM_SHA" > "$TEST_OUTPUT_DIR/sha-documents.json" || return 1
    stream_documents "$ORDER_STREAM_DATE" > "$TEST_OUTPUT_DIR/date-documents.json" || return 1
    jq -e 'length == 2' "$TEST_OUTPUT_DIR/sha-documents.json" >/dev/null || {
        log_fail "SHA-order target did not contain the two expected documents"
        return 1
    }
    cmp -s "$TEST_OUTPUT_DIR/sha-documents.json" "$TEST_OUTPUT_DIR/date-documents.json" || {
        diff -u "$TEST_OUTPUT_DIR/sha-documents.json" "$TEST_OUTPUT_DIR/date-documents.json" >&2 || true
        log_fail "SHA-order and capture-date-order targets differ"
        return 1
    }
}

run_test test_replay_order_independence
