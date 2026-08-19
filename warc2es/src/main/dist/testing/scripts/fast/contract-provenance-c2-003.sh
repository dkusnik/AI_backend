#!/bin/bash
# OWNER: C2-003
# Provenance decorator, stable identity, and conflict validation contract.
source "$(dirname "$0")/../../lib/test-lib.sh"

decorator="$PROJECT_ROOT/src/main/java/pl/gov/nac/warc/processors/WarcDecoratorProvenance.java"
exporter="$PROJECT_ROOT/src/main/java/pl/gov/nac/warc/consumers/ElasticsearchExporterVT.java"
case_identity() {
    assert_file_exists "$decorator" || return 1
    if ! grep -Fq 'effectiveFirstSeen' "$exporter" || grep -Fq 'ingestStartDate, meta.date' "$exporter"; then
        log_fail "WET identity still uses invocation start date or lacks effective-first-seen"
        return 1
    fi
}
case_conflict_validation() {
    if ! grep -Fq 'provenance mismatch' "$exporter" || ! grep -Fq 'integrity' "$exporter"; then
        log_fail "conflicting content/provenance is not rejected"
        return 1
    fi
}

run_stage "URI plus effective-first-seen identity" case_identity || true
run_stage "same-id content/provenance conflict validation" case_conflict_validation || true
finish_stages
