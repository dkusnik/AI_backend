#!/bin/bash
# tc-perf-001-throughput.sh
source "$(dirname "$0")/../../lib/test-lib.sh"

measure_throughput() {
    log_info "BENCHMARK: Throughput (Target > 500MB/s)"

    local source_warc="${TEST_DATA_DIR}/tiny.warc.gz"
    local bench_warc="${TEST_OUTPUT_DIR}/bench-100m.warc.gz"
    local out_warc="${TEST_OUTPUT_DIR}/bench-out.wet.gz"
    mkdir -p "$TEST_OUTPUT_DIR"

    log_info "Generating synthetic benchmark file..."
    cp "$source_warc" "$bench_warc"
    for i in {1..17}; do
        cat "$bench_warc" "$bench_warc" > "${bench_warc}.tmp" && mv "${bench_warc}.tmp" "$bench_warc"
    done

    local size_bytes
    size_bytes=$(stat -c %s "$bench_warc")
    local size_mb
    size_mb=$(awk -v b="$size_bytes" 'BEGIN { printf "%.2f", b/1024/1024 }')
    log_info "Generated file size: ${size_mb} MB"

    local start_time end_time duration throughput
    start_time=$(date +%s.%N)
    "$WARC_CLI" extract-text "$bench_warc" "$out_warc" --silent >/dev/null 2>&1 || {
      log_fail "Benchmark run failed"
      return 1
    }
    end_time=$(date +%s.%N)

    duration=$(awk -v s="$start_time" -v e="$end_time" 'BEGIN { d=e-s; if (d<=0) d=0.001; printf "%.6f", d }')
    throughput=$(awk -v mb="$size_mb" -v d="$duration" 'BEGIN { printf "%.2f", mb/d }')

    log_info "Processed ${size_mb} MB in ${duration} seconds."
    log_success "Throughput: ${throughput} MB/s"

    if awk -v t="$throughput" 'BEGIN { exit (t<=500) }'; then
      log_success "Target met (> 500 MB/s)"
    else
      log_warn "Below target (expected > 500 MB/s, got ${throughput} MB/s)."
    fi

    return 0
}

measure_throughput
