#!/bin/bash
# tc-merge-008-temporal-monotonicity.sh
# T-095: X-NAC-First-Seen <= X-NAC-Last-Seen for every record in merged output.
# Uses verify_base.doet.gz (TC-01 validated, 398 records).
set -euo pipefail
source "$(dirname "$0")/../../../../lib/test-lib.sh"

test_merge_temporal_monotonicity() {
    ensure_test_data "verify_base.doet.gz" || { log_warn "Fixture missing — skipping"; return 0; }

    log_info "Checking X-NAC-First-Seen <= X-NAC-Last-Seen in verify_base.doet.gz..."

    local violations
    violations=$(zcat "$TEST_DATA_DIR/verify_base.doet.gz" | python3 - <<'EOF'
import sys

first_seen = None
violations = 0
record_num = 0

for line in sys.stdin:
    line = line.rstrip('\n')
    if line == 'WARC/1.0':
        first_seen = None
        last_seen = None
        record_num += 1
    lower = line.lower()
    if lower.startswith('x-nac-first-seen:'):
        first_seen = line.split(':', 1)[1].strip()
    elif lower.startswith('x-nac-last-seen:'):
        last_seen = line.split(':', 1)[1].strip()
        if first_seen and last_seen and first_seen > last_seen:
            violations += 1
            print(f"VIOLATION record {record_num}: first={first_seen} > last={last_seen}", file=sys.stderr)

print(violations)
EOF
)

    log_info "Temporal violations: $violations"

    if [[ "$violations" -ne 0 ]]; then
        log_fail "Found $violations records where X-NAC-First-Seen > X-NAC-Last-Seen"
        echo "TESTCASE|temporal-monotonicity|FAIL|violations=$violations"
        return 1
    fi

    local pair_count
    pair_count=$(zcat "$TEST_DATA_DIR/verify_base.doet.gz" | grep -c "^X-NAC-First-Seen:" || echo 0)
    log_info "Checked $pair_count records: all First-Seen <= Last-Seen ✓"
    echo "TESTCASE|temporal-monotonicity|PASS|checked=$pair_count,violations=0"
}

run_test test_merge_temporal_monotonicity
