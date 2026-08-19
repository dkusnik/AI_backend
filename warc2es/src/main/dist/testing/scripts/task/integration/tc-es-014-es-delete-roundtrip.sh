#!/bin/bash
# TB-L1: Live publish, exact delete, republish, and empty reinit cycle.
set -euo pipefail
source "$(dirname "$0")/../../../lib/test-lib.sh"

ES_URL="${ES_URL:-http://localhost:9200}"
TB_L1_STREAM=""

cleanup_stream() {
    [[ -n "$TB_L1_STREAM" ]] || return 0
    ES_URL="$ES_URL" "$ES_CLI" delete-stream "$TB_L1_STREAM" &>/dev/null || true
}
trap cleanup_stream EXIT

es_count() {
    local target="$1"
    local auth_args=()
    [[ -n "${ES_PASS:-}" ]] && auth_args=(-u "${ES_USER:-elastic}:${ES_PASS}")

    curl -fsS "${auth_args[@]}" "$ES_URL/$target/_count" \
        | python3 -c 'import json,sys; print(json.load(sys.stdin)["count"])'
}

snapshot_archive() {
    local root="$1" output="$2" file
    : > "$output"
    while IFS= read -r -d '' file; do
        stat -c '%n %F %a %s %Y' -- "$file" >> "$output" || return 1
        [[ -f "$file" ]] && sha256sum -- "$file" >> "$output"
    done < <(find "$root" -mindepth 1 -print0 | LC_ALL=C sort -z)
}

setup_mvp_runtime() {
    local runtime="$1"
    local deployed_root="$PROJECT_ROOT/out"

    [[ -x "$deployed_root/app/bin/es-cli" ]] || {
        log_fail "Missing packaged MVP application: $deployed_root/app/bin/es-cli"
        return 1
    }

    mkdir -p "$runtime"
    ln -s "$deployed_root/app" "$runtime/app"
    cp "$deployed_root/es-upsert.sh" "$runtime/es-upsert.sh"
    cp "$deployed_root/es-delete.sh" "$runtime/es-delete.sh"
    cp "$deployed_root/es-reinit.sh" "$runtime/es-reinit.sh"
    chmod +x "$runtime/es-upsert.sh" "$runtime/es-delete.sh" "$runtime/es-reinit.sh"
}

make_wet() {
    local output="$1" url_id="$2" crawl_id="$3" label="$4"
    local content="delete-roundtrip-$label"
    printf 'WARC/1.0\r\nWARC-Type: conversion\r\nWARC-Target-URI: https://example.test/delete-roundtrip/%s\r\nWARC-Date: 2026-07-10T01:01:01Z\r\nWARC-Record-ID: <urn:uuid:delete-roundtrip-%s>\r\nWARC-Block-Digest: sha256:delete-roundtrip-%s\r\nX-NAC-First-Seen: 2026-07-10\r\nX-NAC-URL-ID: %s\r\nX-NAC-Crawl-ID: %s\r\nContent-Type: text/plain\r\nContent-Length: %s\r\n\r\n%s\r\n\r\n' \
        "$label" "$label" "$label" "$url_id" "$crawl_id" \
        "${#content}" "$content" | gzip > "$output"
}

test_es_delete_roundtrip() {
    if [[ "${RUN_DESTRUCTIVE_ES_TESTS:-false}" != true ]]; then
        log_warn "Skipping destructive Elasticsearch roundtrip; set RUN_DESTRUCTIVE_ES_TESTS=true"
        return 0
    fi
    if ! ES_URL="$ES_URL" "$ES_CLI" check-health &>/dev/null; then
        if [[ "${REQUIRE_ES:-false}" == true ]]; then
            log_fail "Elasticsearch is required for the destructive delete/reinit acceptance test"
            return 1
        fi
        log_warn "Elasticsearch not available - skipping"
        return 0
    fi

    local runtime="$TEST_OUTPUT_DIR/mvp-runtime"
    local suffix stream_id stream
    local url_a crawl_a url_b crawl_b
    local source_a="$TEST_OUTPUT_DIR/delete-roundtrip-a.wet.gz"
    local source_b="$TEST_OUTPUT_DIR/delete-roundtrip-b.wet.gz"
    local digest_a digest_b archived_a archived_b
    local before_reinit="$TEST_OUTPUT_DIR/archive-before-reinit"
    local after_reinit="$TEST_OUTPUT_DIR/archive-after-reinit"
    local published_count deleted_count republished_count reinit_count

    IFS= read -r suffix < /proc/sys/kernel/random/uuid
    suffix="${suffix//-/}"
    stream_id="tb_l1_$suffix"
    stream="nac-data-$stream_id"
    url_a="tbl1urla$suffix"
    crawl_a="tbl1crawla$suffix"
    url_b="tbl1urlb$suffix"
    crawl_b="tbl1crawlb$suffix"

    if ES_URL="$ES_URL" "$ES_CLI" get-stream "$stream" &>/dev/null; then
        log_fail "High-entropy test stream unexpectedly already exists: $stream"
        return 1
    fi
    TB_L1_STREAM="$stream"

    setup_mvp_runtime "$runtime" || return 1
    make_wet "$source_a" "$url_a" "$crawl_a" a
    make_wet "$source_b" "$url_b" "$crawl_b" b
    digest_a=$(sha256sum -- "$source_a" | awk '{print $1}')
    digest_b=$(sha256sum -- "$source_b" | awk '{print $1}')
    archived_a="$runtime/all/wet/$url_a/$crawl_a/$digest_a.wet.gz"
    archived_b="$runtime/all/wet/$url_b/$crawl_b/$digest_b.wet.gz"

    "$runtime/es-reinit.sh" --stream="$stream_id" --es-url="$ES_URL" --yes >/dev/null
    "$runtime/es-upsert.sh" "$source_a" --stream="$stream_id" \
        --url-id="$url_a" --crawl-id="$crawl_a" --es-url="$ES_URL"
    "$runtime/es-upsert.sh" "$source_b" --stream="$stream_id" \
        --url-id="$url_b" --crawl-id="$crawl_b" --es-url="$ES_URL"
    [[ -f "$archived_a" && -f "$archived_b" && -f "$source_a" && -f "$source_b" ]] || {
        log_fail "Two-pair publish did not preserve sources and archive both WETs"
        return 1
    }

    ES_URL="$ES_URL" "$ES_CLI" refresh "$stream" >/dev/null
    published_count="$(es_count "$stream")"
    [[ "$published_count" -eq 2 ]] || {
        log_fail "Two-pair publish expected 2 documents, got $published_count"
        return 1
    }

    "$runtime/es-delete.sh" --stream="$stream_id" --url-id="$url_a" \
        --crawl-id="$crawl_a" --es-url="$ES_URL"
    [[ ! -e "$archived_a" && -f "$archived_b" && -f "$source_a" ]] || {
        log_fail "Targeted delete did not remove only pair A's published WET"
        return 1
    }
    ES_URL="$ES_URL" "$ES_CLI" refresh "$stream" >/dev/null
    deleted_count="$(es_count "$stream")"
    [[ "$deleted_count" -eq 1 ]] || {
        log_fail "Targeted pair delete expected pair B only, got $deleted_count documents"
        return 1
    }

    "$runtime/es-upsert.sh" "$source_a" --stream="$stream_id" \
        --url-id="$url_a" --crawl-id="$crawl_a" --es-url="$ES_URL"
    [[ -f "$archived_a" && -f "$archived_b" ]] || {
        log_fail "Republish did not restore the two-pair archive"
        return 1
    }
    ES_URL="$ES_URL" "$ES_CLI" refresh "$stream" >/dev/null
    republished_count="$(es_count "$stream")"
    [[ "$republished_count" -eq "$published_count" ]] || {
        log_fail "Republish count changed: expected $published_count, got $republished_count"
        return 1
    }

    snapshot_archive "$runtime/all/wet" "$before_reinit" || return 1
    "$runtime/es-reinit.sh" --stream="$stream_id" --es-url="$ES_URL" --yes >/dev/null
    snapshot_archive "$runtime/all/wet" "$after_reinit" || return 1
    cmp -s "$before_reinit" "$after_reinit" || {
        log_fail "Reinit changed the published WET archive"
        return 1
    }
    ES_URL="$ES_URL" "$ES_CLI" refresh "$stream" >/dev/null
    reinit_count="$(es_count "$stream")"
    [[ "$reinit_count" -eq 0 ]] || {
        log_fail "Reinit replayed or retained $reinit_count documents"
        return 1
    }

    log_info "publish(2) -> targeted-delete(1) -> republish(2) -> reinit(0) passed"
}

run_test test_es_delete_roundtrip
