#!/bin/bash
# tc-es-003-stream-mgmt.sh
source "$(dirname "$0")/../../../../lib/test-lib.sh"

test_stream_mgmt() {
    if ! "$ES_CLI" check-health &> /dev/null; then
        log_warn "Elasticsearch not available. Skipping test."
        return 0
    fi

    local stream="test-stream-mgmt-$(date +%s)"

    # 1. List (verify absent)
    local list_pre
    list_pre=$("$ES_CLI" list-streams)
    if echo "$list_pre" | grep -q "$stream"; then
        log_fail "Stream $stream exists before creation"
        return 1
    fi

    # 2. Load (create) - Need dummy data?
    # Or maybe create-stream command exists?
    # Usually load creates.
    ensure_test_data "tiny.warc.gz" || return 1
    local doet="$TEST_OUTPUT_DIR/es-mgmt.doet.gz"
    "$WARC_CLI" extract-text "$TEST_DATA_DIR/tiny.warc.gz" "$TEST_OUTPUT_DIR/es-mgmt.wet.gz"
    "$WARC_CLI" dedupe "$TEST_OUTPUT_DIR/es-mgmt.wet.gz" "$doet"

    "$ES_CLI" load-stream "$doet" "$stream"

    # 3. List (verify present)
    local list_post
    list_post=$("$ES_CLI" list-streams)
    if echo "$list_post" | grep -q "$stream"; then
        log_success "Stream created"
    else
        log_fail "Stream not created"
    fi

    # 4. Delete
    "$ES_CLI" delete-stream "$stream"

    # 5. List (verify absent)
    local list_final
    list_final=$("$ES_CLI" list-streams)
    if echo "$list_final" | grep -q "$stream"; then
        log_fail "Stream not deleted"
        return 1
    else
        log_success "Stream deleted"
    fi
}

run_test test_stream_mgmt
