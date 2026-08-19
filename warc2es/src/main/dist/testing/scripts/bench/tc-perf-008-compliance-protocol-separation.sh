#!/bin/bash
# tc-perf-008-compliance-protocol-separation.sh
source "$(dirname "$0")/../../lib/test-lib.sh"

compliance_protocol_separation() {
    local bench_dir="$PROJECT_ROOT/src/main/dist/testing/scripts/bench"

    local compliance_scripts=(
        "$bench_dir/tc-perf-004-compliance-b1-throughput.sh"
        "$bench_dir/tc-perf-005-compliance-b2-throughput.sh"
        "$bench_dir/tc-perf-006-compliance-memory.sh"
        "$bench_dir/tc-perf-007-compliance-output.sh"
    )

    local s
    for s in "${compliance_scripts[@]}"; do
        assert_file_exists "$s" || return 1
        if grep -q "@runs:" "$s"; then
            echo "TESTCASE|compliance-protocol-separation|FAIL|$s declares benchmark runs"
            return 1
        fi
    done

    local bench_protocol="$PROJECT_ROOT/src/main/dist/testing/scripts/integration/bench-optimization.sh"
    assert_file_exists "$bench_protocol" || return 1
    assert_contains "7 runs per scenario" "$bench_protocol" || return 1

    echo "TESTCASE|compliance-protocol-separation|PASS|compliance and benchmark protocol separated"
    return 0
}

run_test compliance_protocol_separation
