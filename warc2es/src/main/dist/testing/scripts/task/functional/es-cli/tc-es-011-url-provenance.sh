#!/bin/bash
# tc-es-011-url-provenance.sh
# T-085: Verify that documents from a merge are retrievable by URL and that
# provenance distribution (base-only, merged, new) matches the known merge output.
# Also verifies that diff records (new + merged) are correctly upserted over base.
#
# Full url-scope-merge test (same digest, two URLs) requires pre-built fixtures:
#   shared/url_scope_base.doet.gz  (merge --deduplicate-scope=url output-base)
#   shared/url_scope_diff.doet.gz  (merge --deduplicate-scope=url output-diff)
# When absent, that sub-test is skipped with a generation hint.
#
# Uses verify_base.doet.gz (398 docs: base-only=97, merged=193) and
#       verify_diff.doet.gz (301 docs: merged=193, new=108).
set -euo pipefail
source "$(dirname "$0")/../../../../lib/test-lib.sh"

ES_URL="${ES_URL:-http://localhost:9200}"

# Known counts from TC-01 verify merge (plock.ap.gov.pl)
BASE_ONLY=97
MERGED=193
NEW=108
TOTAL_BASE=398

test_url_provenance() {
    if ! "$ES_CLI" check-health &>/dev/null; then
        log_warn "Elasticsearch not available — skipping"
        return 0
    fi

    ensure_test_data "verify_base.doet.gz" || { log_warn "Fixture verify_base.doet.gz missing — skipping"; return 0; }
    ensure_test_data "verify_diff.doet.gz" || { log_warn "Fixture verify_diff.doet.gz missing — skipping"; return 0; }

    local stream="test-url-prov-$$"
    local failed=0

    # --- Part 1: Load base, verify provenance counts ---
    log_info "Part 1: Loading verify_base.doet.gz (base) into $stream..."
    "$ES_CLI" load-stream "$TEST_DATA_DIR/verify_base.doet.gz" "$stream"
    sleep 2

    local total
    total=$(curl -s "$ES_URL/$stream/_count" -H "Content-Type: application/json" \
        -d '{"query":{"match_all":{}}}' | python3 -c "import sys,json; print(json.load(sys.stdin).get('count',0))")

    if [[ "$total" -ne "$TOTAL_BASE" ]]; then
        log_fail "Base load: expected $TOTAL_BASE docs, got $total"
        failed=$((failed+1))
    else
        log_info "Base total: $total = $TOTAL_BASE ✓"
    fi

    for entry in "base-only:$BASE_ONLY" "merged:$MERGED"; do
        local prov_name="${entry%%:*}"
        local prov_expected="${entry##*:}"
        local prov_count
        prov_count=$(curl -s "$ES_URL/$stream/_count" -H "Content-Type: application/json" -d \
            "{\"query\":{\"term\":{\"nac-merge-result.keyword\":\"$prov_name\"}}}" | \
            python3 -c "import sys,json; print(json.load(sys.stdin).get('count',0))")
        if [[ "$prov_count" -ne "$prov_expected" ]]; then
            log_fail "Provenance '$prov_name': expected $prov_expected, got $prov_count"
            failed=$((failed+1))
        else
            log_info "Provenance '$prov_name': $prov_count = $prov_expected ✓"
        fi
    done

    # --- Part 2: URL retrieval — known domain plock.ap.gov.pl must be searchable ---
    local url_hits
    url_hits=$(curl -s "$ES_URL/$stream/_count" -H "Content-Type: application/json" -d \
        '{"query":{"wildcard":{"warc-uri":"*plock.ap.gov.pl*"}}}' | \
        python3 -c "import sys,json; print(json.load(sys.stdin).get('count',0))")
    if [[ "$url_hits" -eq 0 ]]; then
        log_fail "URL filter returned 0 hits for plock.ap.gov.pl"
        failed=$((failed+1))
    else
        log_info "URL filter (plock.ap.gov.pl): $url_hits docs ✓"
    fi

    # --- Part 3: Load diff (upsert), verify new records appear ---
    log_info "Part 3: Loading verify_diff.doet.gz (diff/upsert) into $stream..."
    "$ES_CLI" load-stream "$TEST_DATA_DIR/verify_diff.doet.gz" "$stream"
    sleep 2

    local total_after_diff
    total_after_diff=$(curl -s "$ES_URL/$stream/_count" -H "Content-Type: application/json" \
        -d '{"query":{"match_all":{}}}' | python3 -c "import sys,json; print(json.load(sys.stdin).get('count',0))")
    # After upsert: total stays at TOTAL_BASE (diff upserts base docs, new adds new ones → still 398)
    if [[ "$total_after_diff" -ne "$TOTAL_BASE" ]]; then
        log_fail "After diff upsert: expected $TOTAL_BASE docs, got $total_after_diff"
        failed=$((failed+1))
    else
        log_info "After diff upsert: $total_after_diff = $TOTAL_BASE ✓"
    fi

    local new_count
    new_count=$(curl -s "$ES_URL/$stream/_count" -H "Content-Type: application/json" -d \
        '{"query":{"term":{"nac-merge-result.keyword":"new"}}}' | \
        python3 -c "import sys,json; print(json.load(sys.stdin).get('count',0))")
    if [[ "$new_count" -ne "$NEW" ]]; then
        log_fail "New records after diff: expected $NEW, got $new_count"
        failed=$((failed+1))
    else
        log_info "New records after diff: $new_count = $NEW ✓"
    fi

    "$ES_CLI" delete-stream "$stream" &>/dev/null || true

    # --- Part 4: url-scope fixture check (optional) ---
    if [[ -f "$TEST_DATA_DIR/url_scope_base.doet.gz" ]]; then
        log_info "Part 4: url-scope fixture found — running url-scope provenance test..."
        local us_stream="test-url-scope-$$"
        "$ES_CLI" load-stream "$TEST_DATA_DIR/url_scope_base.doet.gz" "$us_stream"
        sleep 2
        local us_total
        us_total=$(curl -s "$ES_URL/$us_stream/_count" -H "Content-Type: application/json" \
            -d '{"query":{"match_all":{}}}' | python3 -c "import sys,json; print(json.load(sys.stdin).get('count',0))")
        log_info "url-scope base total: $us_total"
        "$ES_CLI" delete-stream "$us_stream" &>/dev/null || true
        if [[ "$us_total" -eq 0 ]]; then
            log_fail "url-scope base produced 0 docs"
            failed=$((failed+1))
        fi
    else
        log_warn "Part 4 SKIPPED: url_scope_base.doet.gz not in $TEST_DATA_DIR"
        log_warn "To generate: warc-cli merge --deduplicate-scope=url --output-base=shared/url_scope_base.doet.gz --output-diff=shared/url_scope_diff.doet.gz plock1.doet.gz plock2.doet.gz"
    fi

    if [[ $failed -gt 0 ]]; then
        echo "TESTCASE|url-provenance|FAIL|failures=$failed"
        return 1
    fi
    echo "TESTCASE|url-provenance|PASS|base=$TOTAL_BASE,base-only=$BASE_ONLY,merged=$MERGED,new=$NEW"
}

run_test test_url_provenance
