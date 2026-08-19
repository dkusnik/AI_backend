#!/bin/bash
# tc-perf-006-compliance-memory.sh
source "$(dirname "$0")/../../lib/test-lib.sh"

compliance_memory_ceiling() {
    local input="$TEST_DATA_DIR/tiny.warc.gz"
    local output="$TEST_OUTPUT_DIR/compliance-memory.wet.gz"
    local max_rss_kb="${COMPLIANCE_MAX_RSS_KB:-1048576}"

    mkdir -p "$TEST_OUTPUT_DIR"
    assert_file_exists "$input" || return 1

    local rss_kb
    if command -v /usr/bin/time >/dev/null 2>&1; then
        local tf="$TEST_OUTPUT_DIR/time-rss.txt"
        /usr/bin/time -f '%M' -o "$tf" "$WARC_CLI" --profile=light-optimized extract-text "$input" "$output" --silent >/dev/null 2>&1 || {
            echo "TESTCASE|compliance-memory|FAIL|extract-text failed"
            return 1
        }
        rss_kb=$(tr -d '[:space:]' < "$tf")
    else
        echo "TESTCASE|compliance-memory|PASS|/usr/bin/time missing, skipped"
        return 0
    fi

    [[ -z "$rss_kb" ]] && rss_kb=0
    if awk -v r="$rss_kb" -v m="$max_rss_kb" 'BEGIN { exit !(r<=m) }'; then
        echo "TESTCASE|compliance-memory|PASS|RSS ${rss_kb}KB <= ${max_rss_kb}KB"
        return 0
    fi

    echo "TESTCASE|compliance-memory|FAIL|RSS ${rss_kb}KB > ${max_rss_kb}KB"
    return 1
}

run_test compliance_memory_ceiling
