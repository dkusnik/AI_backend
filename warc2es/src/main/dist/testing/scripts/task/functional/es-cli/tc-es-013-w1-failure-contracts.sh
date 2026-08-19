#!/bin/bash
# tc-es-013-w1-failure-contracts.sh
# W1-7: ES HTTP failures are non-zero, and a one-document load reports and
# retrieves exactly one indexed document.
set -euo pipefail
source "$(dirname "$0")/../../../../lib/test-lib.sh"

ES_URL="${ES_URL:-http://localhost:9200}"

es_post_json() {
    local path="$1"
    local json="$2"
    local auth_args=()
    [[ -n "${ES_PASS:-}" ]] && auth_args=(-u "${ES_USER:-elastic}:${ES_PASS}")
    curl -sS -X POST "${auth_args[@]}" "$ES_URL$path" \
        -H "Content-Type: application/json" \
        -d "$json"
}

es_status() {
    local path="$1"
    local auth_args=()
    [[ -n "${ES_PASS:-}" ]] && auth_args=(-u "${ES_USER:-elastic}:${ES_PASS}")
    curl -sS -o /dev/null -w '%{http_code}' "${auth_args[@]}" "$ES_URL$path"
}

test_batch_delete_nonexistent_index_fails() {
    if ! "$ES_CLI" check-health &>/dev/null; then
        log_warn "Elasticsearch not available — skipping"
        return 0
    fi

    local code=0
    "$ES_CLI" batch-delete "w1-missing-index-$$" '{"query":{"match_all":{}}}' \
        > "$TEST_OUTPUT_DIR/batch-delete.out" 2> "$TEST_OUTPUT_DIR/batch-delete.err" || code=$?
    if [[ "$code" -eq 0 ]]; then
        log_fail "batch-delete against nonexistent index unexpectedly succeeded"
        return 1
    fi
    log_info "batch-delete against nonexistent index failed (exit=$code) ✓"
}

test_single_doc_roundtrip_counts_and_search() {
    if ! "$ES_CLI" check-health &>/dev/null; then
        log_warn "Elasticsearch not available — skipping"
        return 0
    fi

    local index="w1-roundtrip-$$"
    local wet="$TEST_OUTPUT_DIR/w1-single.wet"
    local marker="w1-roundtrip-marker-$$"
    local log="$TEST_OUTPUT_DIR/w1-load.log"
    mkdir -p "$TEST_OUTPUT_DIR"

    printf 'WARC/1.0\r\nWARC-Type: conversion\r\nWARC-Target-URI: http://example.test/w1-roundtrip-%s\r\nWARC-Date: 2026-07-08T00:00:00Z\r\nWARC-Record-ID: <urn:uuid:w1-roundtrip-%s>\r\nWARC-Block-Digest: sha256:w1roundtrip%s\r\nContent-Type: text/plain\r\nContent-Length: %s\r\n\r\n%s\r\n\r\n' \
        "$$" "$$" "$$" "${#marker}" "$marker" > "$wet"

    "$ES_CLI" load-index "$wet" "$index" --progress-none --final-report-full > "$log" 2>&1

    local records_in indexed
    records_in=$(awk '/^\[es-exporter-vt\]/{inside=1; next} /^\[/{inside=0} inside && $2=="recordsIn"{print $1}' "$log" | tail -n 1)
    indexed=$(awk '/^\[es-exporter-vt\]/{inside=1; next} /^\[/{inside=0} inside && $2=="indexed"{print $1}' "$log" | tail -n 1)
    if [[ "$records_in" != "1" || "$indexed" != "1" ]]; then
        log_fail "Expected recordsIn=1 and indexed=1, got recordsIn=${records_in:-missing} indexed=${indexed:-missing}"
        "$ES_CLI" batch-delete "$index" '{"query":{"match_all":{}}}' &>/dev/null || true
        return 1
    fi

    "$ES_CLI" refresh "$index" > /dev/null
    es_post_json "/$index/_search?pretty" \
        "$(printf '{"query":{"match_phrase":{"content":"%s"}}}' "$marker")" \
        > "$TEST_OUTPUT_DIR/w1-search.out"
    if ! grep -q "$marker" "$TEST_OUTPUT_DIR/w1-search.out"; then
        log_fail "Indexed document not retrievable by search marker"
        "$ES_CLI" batch-delete "$index" '{"query":{"match_all":{}}}' &>/dev/null || true
        return 1
    fi

    "$ES_CLI" batch-delete "$index" '{"query":{"match_all":{}}}' &>/dev/null || true
    log_info "single-doc round-trip indexed=recordsIn=1 and search found marker ✓"
}

test_stream_purge_preserves_shared_audit_index() {
    if ! "$ES_CLI" check-health &>/dev/null; then
        log_warn "Elasticsearch not available — skipping"
        return 0
    fi

    local stream_id="w3-purge-$$"
    local stream="nac-data-$stream_id"

    "$ES_CLI" init "$stream" >/dev/null
    if ! "$ES_CLI" get-stream "$stream" >/dev/null; then
        log_fail "get-stream failed on a freshly created stream (require_args regression direction)"
        "$ES_CLI" delete-stream "$stream" &>/dev/null || true
        return 1
    fi
    "$ES_CLI" purge "$stream" >/dev/null

    if [[ "$(es_status '/.project-elm-audit')" != "200" ]]; then
        log_fail "Stream-scoped purge deleted the shared audit index"
        return 1
    fi
    if "$ES_CLI" get-stream "$stream" &>/dev/null; then
        log_fail "Stream-scoped purge left $stream behind"
        "$ES_CLI" delete-stream "$stream" &>/dev/null || true
        return 1
    fi
    log_info "stream-scoped purge preserved the shared audit index ✓"
}

run_test test_batch_delete_nonexistent_index_fails
run_test test_single_doc_roundtrip_counts_and_search
run_test test_stream_purge_preserves_shared_audit_index
