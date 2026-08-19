#!/bin/bash
# tc-es-001-load-stream.sh
set -euo pipefail
source "$(dirname "$0")/../../../../lib/test-lib.sh"

ES_URL="${ES_URL:-http://localhost:9200}"

assert_warc_output_exists() {
    local path="$1"
    if [[ -f "$path" ]]; then
        return 0
    fi
    if [[ -d "$path" ]] && find "$path" -maxdepth 1 -type f -name '*.gz' | grep -q .; then
        return 0
    fi
    log_fail "WARC output not found: $path"
    return 1
}

es_index_count() {
    local index="$1"
    curl -sS "$ES_URL/$index/_count" \
        | python3 -c "import json,sys; print(int(json.load(sys.stdin).get('count',0)))" 2>/dev/null \
        || echo "0"
}

cleanup_index() {
    local index="$1"
    curl -sS -o /dev/null -X DELETE "$ES_URL/$index" || true
}

test_load_stream() {
    # Prerequisite: ES must be running
    if ! "$ES_CLI" check-health &> /dev/null; then
        log_warn "Elasticsearch not available. Skipping test."
        echo "TESTCASE|load-stream-es-unavailable|SKIP|reason=no-es"
        return 0
    fi

    # Needs extractable HTML content; tiny.warc.gz can validly produce 0 records.
    ensure_test_data "example.com.warc.gz" || return 1
    local input="$TEST_DATA_DIR/example.com.warc.gz"
    local wet="$TEST_OUTPUT_DIR/es-001.wet.gz"
    local doet="$TEST_OUTPUT_DIR/es-001.doet.gz"
    local stream="test-stream-$(date +%s)"

    log_info "Preparing DOET data..."
    "$WARC_CLI" extract-text "$input" "$wet" || return 1
    assert_warc_output_exists "$wet" || return 1
    "$WARC_CLI" dedupe "$wet" "$doet" || return 1
    assert_warc_output_exists "$doet" || return 1

    log_info "Loading stream $stream..."
    "$ES_CLI" load-stream "$doet" "$stream" || return 1

    "$ES_CLI" refresh "$stream" > /dev/null || return 1

    local count
    count=$(es_index_count "$stream")
    if [[ "$count" -gt 0 ]]; then
        log_success "Stream $stream loaded $count documents"
    else
        log_fail "Stream $stream has no indexed documents after load"
        cleanup_index "$stream"
        return 1
    fi

    # Cleanup
    cleanup_index "$stream"
}

run_test test_load_stream
