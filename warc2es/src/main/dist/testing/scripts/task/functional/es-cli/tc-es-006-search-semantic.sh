#!/bin/bash
# tc-es-006-search-semantic.sh
# T-097: Known terms from ingested merged content are retrievable via search;
# retrieved documents carry expected provenance values.
# Uses pre-merged verify_base.doet.gz (plock.ap.gov.pl, 398 records).
set -euo pipefail
source "$(dirname "$0")/../../../../lib/test-lib.sh"

ES_URL="${ES_URL:-http://localhost:9200}"

test_search_semantic() {
    if ! "$ES_CLI" check-health &>/dev/null; then
        log_warn "Elasticsearch not available — skipping"
        return 0
    fi

    ensure_test_data "verify_base.doet.gz" || { log_warn "Fixture verify_base.doet.gz missing in $TEST_DATA_DIR — skipping"; return 0; }

    local stream="test-search-semantic-$$"

    log_info "Loading verify_base.doet.gz into stream $stream..."
    "$ES_CLI" load-stream "$TEST_DATA_DIR/verify_base.doet.gz" "$stream"
    sleep 2

    # 1. Term search: "plock" must return hits (content is from plock.ap.gov.pl)
    log_info "Searching for 'plock' in $stream..."
    local result hits
    result=$(curl -s "$ES_URL/$stream/_search?size=5" \
        -H "Content-Type: application/json" \
        -d '{"query":{"multi_match":{"query":"plock","fields":["warc-uri","content"]}}}')
    hits=$(echo "$result" | python3 -c "import sys,json; print(json.load(sys.stdin).get('hits',{}).get('total',{}).get('value',0))" 2>/dev/null || echo "0")

    if [[ "$hits" -eq 0 ]]; then
        log_fail "Search for 'plock' returned 0 hits — content not retrievable"
        "$ES_CLI" delete-stream "$stream" &>/dev/null || true
        echo "TESTCASE|search-semantic|FAIL|hits=0"
        return 1
    fi
    log_info "Search hits for 'plock': $hits ✓"

    # 2. Verify returned docs have nac-merge-result set to a known value
    local bad_prov
    bad_prov=$(echo "$result" | python3 -c "
import sys, json
d = json.load(sys.stdin)
docs = d.get('hits',{}).get('hits',[])
valid = {'base-only','merged','new','uri-changed','uri-reverted'}
bad = [doc.get('_source',{}).get('nac-merge-result','MISSING') for doc in docs
       if doc.get('_source',{}).get('nac-merge-result','MISSING') not in valid]
print(len(bad))
" 2>/dev/null || echo "0")

    if [[ "$bad_prov" != "0" ]]; then
        log_fail "$bad_prov returned docs have unexpected or missing nac-merge-result"
        "$ES_CLI" delete-stream "$stream" &>/dev/null || true
        echo "TESTCASE|search-semantic|FAIL|bad-provenance=$bad_prov"
        return 1
    fi
    log_info "All returned docs have valid nac-merge-result values ✓"

    # 3. Provenance distribution: at least one of base-only, merged, or new must be present
    local prov_count
    prov_count=$(curl -s "$ES_URL/$stream/_count" -H "Content-Type: application/json" -d \
        '{"query":{"terms":{"nac-merge-result.keyword":["base-only","merged","new"]}}}' | \
        python3 -c "import sys,json; print(json.load(sys.stdin).get('count',0))")
    if [[ "$prov_count" -eq 0 ]]; then
        log_fail "No documents with standard merge provenance (base-only/merged/new)"
        "$ES_CLI" delete-stream "$stream" &>/dev/null || true
        echo "TESTCASE|search-semantic|FAIL|prov-count=0"
        return 1
    fi
    log_info "Documents with standard merge provenance: $prov_count ✓"

    "$ES_CLI" delete-stream "$stream" &>/dev/null || true
    echo "TESTCASE|search-semantic|PASS|hits=$hits,prov=$prov_count"
}

run_test test_search_semantic
