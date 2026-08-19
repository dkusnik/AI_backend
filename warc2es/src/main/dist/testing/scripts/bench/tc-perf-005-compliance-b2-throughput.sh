#!/bin/bash
# tc-perf-005-compliance-b2-throughput.sh
source "$(dirname "$0")/../../lib/test-lib.sh"

compliance_b2_throughput() {
    local input="$TEST_DATA_DIR/tiny.warc.gz"
    local output="$TEST_OUTPUT_DIR/compliance-b2.warc.gz"
    local min_bps="${COMPLIANCE_B2_MIN_BPS:-1}"

    mkdir -p "$TEST_OUTPUT_DIR"
    assert_file_exists "$input" || return 1

    local size_bytes start end elapsed throughput_bps
    size_bytes=$(stat -c %s "$input")

    start=$(date +%s.%N)
    "$WARC_CLI" --profile=light-optimized convert "$input" "$output" --silent >/dev/null 2>&1 || {
        echo "TESTCASE|compliance-b2-throughput|FAIL|convert failed"
        return 1
    }
    end=$(date +%s.%N)

    elapsed=$(awk -v s="$start" -v e="$end" 'BEGIN { d=e-s; if (d<=0) d=0.001; printf "%.6f", d }')
    throughput_bps=$(awk -v b="$size_bytes" -v d="$elapsed" 'BEGIN { printf "%.2f", b/d }')

    if awk -v t="$throughput_bps" -v m="$min_bps" 'BEGIN { exit !(t>=m) }'; then
        echo "TESTCASE|compliance-b2-throughput|PASS|${throughput_bps}B/s >= ${min_bps}B/s"
        return 0
    fi

    echo "TESTCASE|compliance-b2-throughput|FAIL|${throughput_bps}B/s < ${min_bps}B/s"
    return 1
}

run_test compliance_b2_throughput
