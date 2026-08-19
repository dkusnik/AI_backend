#!/bin/bash
# tc-golden-002-directory-output.sh — Directory-based golden cycle (MULTI_WARC)
# Uses the default MULTI_WARC output format: extract-text produces a directory
# with date-bucketed .doet.gz files.  The directory-aware warc_count() in
# test-lib.sh sums counts across all files in the directory.
# See tc-golden-001 for the file-based variant (single-file output).
# Sequence: purge -> stage1 -> stage2 -> purge -> stage3 -> recalculate counts.
# @timeout: 900
set -euo pipefail
source "$(dirname "$0")/../../lib/test-lib.sh"

# T-203: Per-run isolation — unique index and cleanup on exit.
RUN_ID="${RUN_ID:-$(date +%Y%m%d-%H%M%S)-$$}"
GOLDEN_INDEX="${GOLDEN_INDEX:-nac-data-golden-dir-$RUN_ID}"
GOLDEN_DIR="$PROJECT_ROOT/target/testing/tmp/golden-dir"
mkdir -p "$GOLDEN_DIR"

PLOCK1="$TEST_DATA_DIR/plock.ap.gov.pl.warc.gz"
PLOCK2="$TEST_DATA_DIR/plock.ap.gov.pl-2026-01-30-ace2d026-00000.warc.gz"
WET1="$GOLDEN_DIR/plock1.wet.gz"
DOET1="$GOLDEN_DIR/plock1.doet.gz"
WET2="$GOLDEN_DIR/plock2.wet.gz"
DOET2="$GOLDEN_DIR/plock2.doet.gz"
MERGED="$GOLDEN_DIR/plock-merged.doet.gz"
DIFF="$GOLDEN_DIR/plock-diff.doet.gz"
SENTINEL="$GOLDEN_DIR/.run-id"

ES_URL="${ES_URL:-http://localhost:9200}"

# Static fixture expectations (shared/plock fixtures)
# WET counts assume --deduplicate-scope=global (deduped at extraction time).
# PLOCK2 SHA-256: e7439e6e201225ab4c462bc7909306ef28437bbcd747d4440a93b032773b7749.
# a368cc1 correctly changes WET2 301 -> 300: sort-only still emits both
# /category/uncategorized/page/15/ and /author/admin/page/15/, but normalized
# screen-reader fragments make their 913-byte payloads identical; global dedup
# keeps the shorter author URI. Parent/change global counts: 301/300; sort-only: 465/465.
EXPECTED_WET1=290
EXPECTED_DOET1=291
EXPECTED_WET2=300
EXPECTED_DOET2=301
EXPECTED_MERGED=398
EXPECTED_DIFF=301

# T-203: Cleanup guard — delete per-run data stream on exit (success or failure).
trap 'curl -sS -o /dev/null -X DELETE "${ES_URL}/_data_stream/${GOLDEN_INDEX}" 2>/dev/null || true; rm -rf "$GOLDEN_DIR"' EXIT INT TERM

# T-204: warc_count is defined in test-lib.sh (case-insensitive, WARC 1.0+1.1).
# Directory-aware: sums counts from all *.gz files inside a MULTI_WARC directory.

assert_exact_count() {
    local label="$1"
    local actual="$2"
    local expected="$3"
    if [[ "$actual" -ne "$expected" ]]; then
        log_fail "$label count mismatch: actual=$actual expected=$expected"
        return 1
    fi
    log_info "$label count OK: $actual"
}

# T-206: ES health preflight — cluster must be reachable and non-red.
es_health_check() {
    local body code tmp
    tmp="$(mktemp "$PROJECT_ROOT/target/testing/tmp/es-health.XXXXXX")"
    code=$(curl -sS -o "$tmp" -w "%{http_code}" --connect-timeout 5 --max-time 10 \
        "${ES_URL}/_cluster/health" 2>/dev/null || echo "000")
    if [[ "$code" != "200" ]]; then
        rm -f "$tmp"
        log_warn "ES cluster unreachable at $ES_URL (HTTP $code). Skipping golden ES-dependent test."
        return 2
    fi
    body=$(cat "$tmp"); rm -f "$tmp"
    local status
    status=$(echo "$body" | python3 -c "import json,sys; print(json.load(sys.stdin).get('status','unknown'))" 2>/dev/null || echo "unknown")
    if [[ "$status" == "red" || "$status" == "unknown" ]]; then
        log_warn "ES cluster status is '$status'. Skipping golden ES-dependent test."
        return 2
    fi
    log_info "ES cluster health: $status ($ES_URL, index: $GOLDEN_INDEX)"
}

es_doc_count() {
    local index="$1"
    local body code tmp
    tmp="$(mktemp "$PROJECT_ROOT/target/testing/tmp/bench.XXXXXX")"
    code=$(curl -sS -o "$tmp" -w "%{http_code}" "${ES_URL}/${index}/_count" || echo "000")
    if [[ "$code" != "200" ]]; then
        rm -f "$tmp"
        echo "0"
        return 0
    fi
    body=$(cat "$tmp"); rm -f "$tmp"
    echo "$body" | python3 -c "import json,sys; print(int(json.load(sys.stdin).get('count',0)))" 2>/dev/null || echo "0"
}

es_search_total() {
    local index="$1"
    local term="$2"
    local body code tmp
    tmp="$(mktemp "$PROJECT_ROOT/target/testing/tmp/search.XXXXXX")"
    code=$(curl -sS -o "$tmp" -w "%{http_code}" -X POST "${ES_URL}/${index}/_search" \
        -H "Content-Type: application/json" \
        -d "$(printf '{"size":0,"query":{"multi_match":{"query":"%s","fields":["warc-uri","content"]}}}' "$term")" || echo "000")
    body=$(cat "$tmp")
    rm -f "$tmp"
    if [[ "$code" != "200" ]]; then
        log_fail "Search /${index}/_search failed (HTTP $code): $body"
        return 1
    fi
    echo "$body" | python3 -c "import json,sys; print(int(json.load(sys.stdin).get('hits',{}).get('total',{}).get('value',0)))" 2>/dev/null || echo "0"
}

es_purge_index() {
    local code
    code=$(curl -sS -o /dev/null -w "%{http_code}" -X DELETE \
        "${ES_URL}/_data_stream/${GOLDEN_INDEX}" || echo "000")
    [[ "$code" == "200" || "$code" == "404" ]] || {
        log_fail "DELETE /_data_stream/${GOLDEN_INDEX} failed (HTTP $code)"
        return 1
    }
    sleep 1
    return 0
}

es_refresh_index() {
    curl -sS -o /dev/null -X POST "${ES_URL}/${GOLDEN_INDEX}/_refresh" || true
}

# T-206: Run ES health check once at startup before any stage executes.
set +e
es_health_check
health_rc=$?
set -e
if [[ "$health_rc" -eq 2 ]]; then
    echo "TESTCASE|golden-dir-es-unavailable|SKIP|reason=no-es"
    exit 0
fi
if [[ "$health_rc" -ne 0 ]]; then
    exit 1
fi

stage_purge_before_stage1() {
    log_info "Purging index before Stage 1: $GOLDEN_INDEX"
    es_purge_index || return 1
    local count
    count=$(es_doc_count "$GOLDEN_INDEX")
    [[ "$count" -eq 0 ]] || { log_fail "Index not empty after purge: $count"; return 1; }
    # T-203: Clear RocksDB dedup state to prevent cross-run interference.
    local rocksdb_path="${WARC_DIST_DIR:-${DIST_ROOT:-$PROJECT_ROOT/target/dist}}/var/db/doet"
    if [[ -d "$rocksdb_path" ]]; then
        rm -rf "$rocksdb_path"
        log_info "Cleared RocksDB dedup state: $rocksdb_path"
    fi
}

stage_1_initial_cycle() {
    ensure_test_data "$(basename "$PLOCK1")" || return 1

    # Default MULTI_WARC output (directory with date-bucketed files).
    "$WARC_CLI" extract-text "$PLOCK1" "$WET1" --deduplicate-scope=global

    # WET1 is now a directory; warc_count handles this transparently.
    [[ -d "$WET1" ]] || { log_fail "Expected directory output: $WET1"; return 1; }
    local wet1_file_count
    wet1_file_count=$(find "$WET1" -maxdepth 1 -type f | wc -l)
    log_info "MULTI_WARC directory created: $WET1 (${wet1_file_count} files)"

    "$WARC_CLI" dedupe "$WET1" "$DOET1"

    local wet_count doet_count
    wet_count=$(warc_count "$WET1")
    doet_count=$(warc_count "$DOET1")
    assert_exact_count "plock1.wet.gz (dir)" "$wet_count" "$EXPECTED_WET1" || return 1
    assert_exact_count "plock1.doet.gz" "$doet_count" "$EXPECTED_DOET1" || return 1

    # T-207: Write run-id sentinel so stage2 can verify artifact freshness.
    echo "$RUN_ID" > "$SENTINEL"

    "$ES_CLI" load-stream "$DOET1" "$GOLDEN_INDEX"
    sleep 1
    es_refresh_index

    STAGE1_DOCS=$(es_doc_count "$GOLDEN_INDEX")
    [[ "$STAGE1_DOCS" -gt 0 ]] || { log_fail "Stage 1 ES doc count is zero"; return 1; }
    log_info "Stage 1 ES docs: $STAGE1_DOCS"
    echo "$STAGE1_DOCS" > "$GOLDEN_DIR/plock1-doc-count.txt"
}

stage_2_incremental_cycle() {
    ensure_test_data "$(basename "$PLOCK2")" || return 1

    # T-207: Stale-artifact guard — sentinel must exist and match current RUN_ID.
    if [[ ! -f "$SENTINEL" ]]; then
        log_fail "Missing run sentinel '$SENTINEL' — stage1 must run before stage2."
        return 1
    fi
    local sentinel_id
    sentinel_id=$(cat "$SENTINEL")
    if [[ "$sentinel_id" != "$RUN_ID" ]]; then
        log_fail "Stale artifact: sentinel run-id '$sentinel_id' != current '$RUN_ID'. Re-run from stage1."
        return 1
    fi

    # Default MULTI_WARC output (directory with date-bucketed files).
    "$WARC_CLI" extract-text "$PLOCK2" "$WET2" --deduplicate-scope=global

    [[ -d "$WET2" ]] || { log_fail "Expected directory output: $WET2"; return 1; }
    local wet2_file_count
    wet2_file_count=$(find "$WET2" -maxdepth 1 -type f | wc -l)
    log_info "MULTI_WARC directory created: $WET2 (${wet2_file_count} files)"

    "$WARC_CLI" dedupe "$WET2" "$DOET2"
    "$WARC_CLI" merge --output-base="$MERGED" --output-diff="$DIFF" "$DOET1" "$DOET2"

    local wet_count doet_count merged_count diff_count before after
    wet_count=$(warc_count "$WET2")
    doet_count=$(warc_count "$DOET2")
    merged_count=$(warc_count "$MERGED")
    diff_count=$(warc_count "$DIFF")

    assert_exact_count "plock2.wet.gz (dir)" "$wet_count" "$EXPECTED_WET2" || return 1
    assert_exact_count "plock2.doet.gz" "$doet_count" "$EXPECTED_DOET2" || return 1
    assert_exact_count "plock-merged.doet.gz" "$merged_count" "$EXPECTED_MERGED" || return 1
    assert_exact_count "plock-diff.doet.gz" "$diff_count" "$EXPECTED_DIFF" || return 1

    before=$(es_doc_count "$GOLDEN_INDEX")
    "$ES_CLI" load-stream "$DIFF" "$GOLDEN_INDEX"
    sleep 1
    es_refresh_index
    after=$(es_doc_count "$GOLDEN_INDEX")

    # T-205: Incremental load must add at least one document.
    [[ "$after" -gt "$before" ]] || { log_fail "Stage 2 ES docs did not grow: before=$before after=$after"; return 1; }
    # T-205: Upper bound — cannot add more records than DIFF contains.
    [[ "$after" -le "$((before + EXPECTED_DIFF))" ]] || {
        log_fail "Stage 2 ES docs grew by more than EXPECTED_DIFF=$EXPECTED_DIFF: before=$before after=$after"
        return 1
    }
    STAGE2_DOCS="$after"
    log_info "Stage 2 ES docs: $STAGE2_DOCS"
    echo "$STAGE2_DOCS" > "$GOLDEN_DIR/plock2-doc-count.txt"
}

stage_purge_before_stage3() {
    log_info "Purging index before Stage 3: $GOLDEN_INDEX"
    es_purge_index || return 1
    local count
    count=$(es_doc_count "$GOLDEN_INDEX")
    [[ "$count" -eq 0 ]] || { log_fail "Index not empty after second purge: $count"; return 1; }
}

stage_3_repopulate() {
    [[ -f "$MERGED" ]] || { log_fail "Missing merged baseline: $MERGED"; return 1; }

    "$ES_CLI" load-stream "$MERGED" "$GOLDEN_INDEX"
    sleep 1
    es_refresh_index

    STAGE3_DOCS=$(es_doc_count "$GOLDEN_INDEX")
    [[ "$STAGE3_DOCS" -gt 0 ]] || { log_fail "Stage 3 ES doc count is zero"; return 1; }

    # T-205: Repopulate from merged should yield EXPECTED_MERGED minus 1 (warcinfo excluded).
    local expected_stage3=$(( EXPECTED_MERGED - 1 ))
    [[ "$STAGE3_DOCS" -eq "$expected_stage3" ]] || {
        log_fail "Stage 3 count ($STAGE3_DOCS) != expected ($expected_stage3 = EXPECTED_MERGED-1): repopulate mismatch"
        return 1
    }

    local total
    total=$(es_search_total "$GOLDEN_INDEX" "archiwum") || return 1
    [[ "$total" -gt 0 ]] || { log_fail "Stage 3 search returned 0 results"; return 1; }
    log_info "Stage 3 search total: $total"
}

stage_recalculate_counts() {
    local c_wet1 c_doet1 c_wet2 c_doet2 c_merged c_diff
    c_wet1=$(warc_count "$WET1")
    c_doet1=$(warc_count "$DOET1")
    c_wet2=$(warc_count "$WET2")
    c_doet2=$(warc_count "$DOET2")
    c_merged=$(warc_count "$MERGED")
    c_diff=$(warc_count "$DIFF")

    assert_exact_count "recalc plock1.wet.gz (dir)" "$c_wet1" "$EXPECTED_WET1" || return 1
    assert_exact_count "recalc plock1.doet.gz" "$c_doet1" "$EXPECTED_DOET1" || return 1
    assert_exact_count "recalc plock2.wet.gz (dir)" "$c_wet2" "$EXPECTED_WET2" || return 1
    assert_exact_count "recalc plock2.doet.gz" "$c_doet2" "$EXPECTED_DOET2" || return 1
    assert_exact_count "recalc plock-merged.doet.gz" "$c_merged" "$EXPECTED_MERGED" || return 1
    assert_exact_count "recalc plock-diff.doet.gz" "$c_diff" "$EXPECTED_DIFF" || return 1

    log_info "Recalculated counts summary: wet1=$c_wet1 doet1=$c_doet1 wet2=$c_wet2 doet2=$c_doet2 merged=$c_merged diff=$c_diff"
    log_info "ES docs summary: stage1=${STAGE1_DOCS:-0} stage2=${STAGE2_DOCS:-0} stage3=${STAGE3_DOCS:-0}"
}

run_stage "purge-before-stage1"  stage_purge_before_stage1
run_stage "stage1-initial"       stage_1_initial_cycle
run_stage "stage2-incremental"   stage_2_incremental_cycle
run_stage "purge-before-stage3"  stage_purge_before_stage3
run_stage "stage3-repopulate"    stage_3_repopulate
run_stage "recalculate-counts"   stage_recalculate_counts
finish_stages
